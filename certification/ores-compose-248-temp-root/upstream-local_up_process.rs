use ores_compose::config_loader::{
    merge_runtime_environment, parse_compose_replica_runtime_policies,
    parse_compose_service_environment_policies, plan_project_replicas, resolve_service_environment,
    ComposeServiceEnvironmentPolicies, ReplicaRuntimePlan, ORES_COMPOSE_SERVICE_ENV,
};
use ores_compose::service_plan::plan_project_shutdown_waves;
use ores_compose::{
    execute_source_checkout, parse_trusted_compose_yaml, plan_project_process_waves_trusted,
    plan_source_checkout, read_trusted_compose_file, ComposeProjectDefinition,
    ComposeSourceDefinition, HealthcheckProcessPlan, ProcessCommandSpec, RoutingLabel,
    RuntimeRequest, SourceCheckoutPlan, TrustedComposeFile, TrustedServiceProcessPlan,
    TrustedWorkingDirectory, COMPOSE_CONFIG_FILE,
};
use ores_locks_and_leases::{LocalFileLock, LocalFileLockOptions};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const SOURCE_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);
static SOURCE_LOCK_OWNER_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static ISOLATION_POLICY_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const TRUSTED_ISOLATION_POLICY_ROOT: &str = "/tmp";

type BuildEnvironments = BTreeMap<RoutingLabel, BTreeMap<String, String>>;

struct RunningService {
    service: String,
    replica_id: String,
    environment: BTreeMap<String, String>,
    child: Child,
    healthcheck: Option<HealthcheckProcessPlan>,
    working_directory: TrustedWorkingDirectory,
    isolation_policy_path: Option<PathBuf>,
}

pub(crate) fn run(config_path: Option<PathBuf>) -> Result<(), String> {
    let config_path = resolve_config_path(config_path);
    let trusted = load_trusted_up_config(&config_path)?;
    let project_root = trusted.repo_root;
    let input = trusted.contents;
    let project = parse_trusted_compose_yaml(&input).map_err(|error| error.to_string())?;

    // Finish all pure config/replica/execution/environment admission before
    // crossing a network boundary for an authored source pin. Service secrets
    // are snapshotted here and never inherited implicitly by child processes.
    let replica_policies = parse_compose_replica_runtime_policies(&input, &project)
        .map_err(|error| error.to_string())?;
    let service_environment_policies = parse_compose_service_environment_policies(&input, &project)
        .map_err(|error| error.to_string())?;
    let mut replica_plans =
        plan_project_replicas(&project, &replica_policies).map_err(|error| error.to_string())?;
    let build_environments =
        admit_service_environments(&project, &service_environment_policies, &mut replica_plans)?;
    preflight_project_execution(&project, &replica_plans)?;
    let shutdown_waves =
        plan_project_shutdown_waves(&project).map_err(|error| error.to_string())?;

    // A source-backed project keeps this lock for its entire runtime lifetime,
    // not only while Git commands run. Otherwise a second `up` could switch the
    // shared worktree underneath already-running processes after materialization.
    let mut source_lock = None;
    let execution_root = if let Some(source) = project.source.as_ref() {
        eprintln!(
            "{{\"event\":\"source_materialization_started\",\"project\":{:?},\"commit\":{:?}}}",
            project.project.as_str(),
            source.commit
        );
        let (root, lock) = materialize_source_checkout_serialized(project_root.clone(), source)?;
        source_lock = Some(lock);
        eprintln!(
            "{{\"event\":\"source_materialized\",\"project\":{:?},\"commit\":{:?}}}",
            project.project.as_str(),
            source.commit
        );
        root
    } else {
        project_root
    };

    // Capture every post-materialization failure as data so a held source lock
    // is explicitly released and a release failure can be preserved alongside
    // the execution failure instead of being discarded by `?`/Drop.
    let execution_result = (|| -> Result<(), String> {
        let waves = plan_project_process_waves_trusted(&execution_root, &project)
            .map_err(|error| error.to_string())?;

        // Revalidate filesystem-backed trusted working-directory evidence only
        // after the exact source tree exists. Pure execution/replica/environment
        // admission completed above, before source-network side effects.
        preflight(&waves, &replica_plans)?;
        let isolation_binary = resolve_process_isolation_binary()?;
        if let Some(binary) = &isolation_binary {
            eprintln!(
                "{{\"event\":\"process_isolation_enabled\",\"backend\":\"ores-proc-isolation\",\"binary\":{:?},\"same_user\":true}}",
                binary
            );
        }
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("could not create local compose runtime: {error}"))?;
        runtime.block_on(run_waves(
            project.project.as_str(),
            &execution_root,
            isolation_binary.as_deref(),
            waves,
            replica_plans,
            build_environments,
            shutdown_waves,
        ))
    })();

    finish_with_source_lock(execution_result, source_lock)
}

fn load_trusted_up_config(config_path: &Path) -> Result<TrustedComposeFile, String> {
    let configured_root = config_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    read_trusted_compose_file(configured_root, config_path).map_err(|error| error.to_string())
}

fn resolve_config_path(path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = path {
        return path;
    }
    if let Some(path) = env::var_os("ORES_COMPOSE_CONFIG") {
        return PathBuf::from(path);
    }
    let preferred = Path::new(COMPOSE_CONFIG_FILE);
    if preferred.exists() {
        return preferred.to_path_buf();
    }
    PathBuf::from("ores-compose.yaml")
}

fn admit_service_environments(
    project: &ComposeProjectDefinition,
    policies: &ComposeServiceEnvironmentPolicies,
    replica_plans: &mut BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
) -> Result<BuildEnvironments, String> {
    let mut build_environments = BTreeMap::new();

    for service in project.services() {
        let policy = policies.policy_for(&service.name);
        if matches!(service.execution.runtime, RuntimeRequest::HostProcess) {
            validate_program_resolution(
                service.name.as_str(),
                "start",
                &service.command,
                &policy.inherit_env,
            )?;
        }
        for command in &service.build {
            validate_program_resolution(
                service.name.as_str(),
                "build",
                command,
                &policy.inherit_env,
            )?;
        }
        if let Some(healthcheck) = &service.healthcheck {
            validate_program_resolution(
                service.name.as_str(),
                "healthcheck",
                &healthcheck.command,
                &policy.inherit_env,
            )?;
        }

        let resolved = resolve_service_environment(&policy, |key| env::var(key).ok())
            .map_err(|error| error.to_string())?;

        let mut build_runtime = BTreeMap::new();
        build_runtime.insert(
            ORES_COMPOSE_SERVICE_ENV.to_string(),
            service.name.as_str().to_string(),
        );
        let build_environment = merge_runtime_environment(&resolved, &build_runtime)
            .map_err(|error| error.to_string())?;

        let replicas = replica_plans.get_mut(&service.name).ok_or_else(|| {
            format!(
                "missing admitted replica runtime plan for service {:?}",
                service.name.as_str()
            )
        })?;
        for replica in replicas {
            replica.environment = merge_runtime_environment(&resolved, &replica.environment)
                .map_err(|error| error.to_string())?;
        }

        build_environments.insert(service.name.clone(), build_environment);
    }

    Ok(build_environments)
}

fn validate_program_resolution(
    service: &str,
    phase: &str,
    argv: &[String],
    inherit_env: &[String],
) -> Result<(), String> {
    let Some(program) = argv.first() else {
        return Err(format!(
            "service {service:?} {phase} command unexpectedly has an empty argv"
        ));
    };
    let is_bare_program = Path::new(program).components().count() == 1;
    let path_is_explicitly_inherited = inherit_env.iter().any(|key| key == "PATH");
    if is_bare_program && !path_is_explicitly_inherited {
        return Err(format!(
            "service {service:?} {phase} uses bare program {program:?}; PATH must be explicitly listed in inherit_env"
        ));
    }
    Ok(())
}

fn preflight_project_execution(
    project: &ComposeProjectDefinition,
    replica_plans: &BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
) -> Result<(), String> {
    for service in project.services() {
        if !matches!(service.execution.runtime, RuntimeRequest::HostProcess) {
            return Err(format!(
                "ores-compose up currently executes host-process services only; service {:?} requested another runtime",
                service.name.as_str()
            ));
        }

        let replicas = replica_plans.get(&service.name).ok_or_else(|| {
            format!(
                "missing admitted replica runtime plan for service {:?}",
                service.name.as_str()
            )
        })?;
        if replicas.len() != service.replicas {
            return Err(format!(
                "service {:?} declares {} replicas but runtime planning produced {}",
                service.name.as_str(),
                service.replicas,
                replicas.len()
            ));
        }
        for (expected_index, replica) in replicas.iter().enumerate() {
            if replica.service != service.name || replica.replica_index != expected_index {
                return Err(format!(
                    "replica runtime evidence does not match service {:?} at index {expected_index}",
                    service.name.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn preflight(
    waves: &[Vec<TrustedServiceProcessPlan>],
    replica_plans: &BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
) -> Result<(), String> {
    for plan in waves.iter().flatten() {
        plan.validate_before_execution()
            .map_err(|error| error.to_string())?;
        if !matches!(plan.plan.execution.runtime, RuntimeRequest::HostProcess) {
            return Err(format!(
                "ores-compose up currently executes host-process services only; service {:?} requested another runtime",
                plan.plan.service.as_str()
            ));
        }

        let replicas = replica_plans.get(&plan.plan.service).ok_or_else(|| {
            format!(
                "missing admitted replica runtime plan for service {:?}",
                plan.plan.service.as_str()
            )
        })?;
        if replicas.len() != plan.plan.replicas {
            return Err(format!(
                "service {:?} declares {} replicas but runtime planning produced {}",
                plan.plan.service.as_str(),
                plan.plan.replicas,
                replicas.len()
            ));
        }
        for (expected_index, replica) in replicas.iter().enumerate() {
            if replica.service != plan.plan.service || replica.replica_index != expected_index {
                return Err(format!(
                    "replica runtime evidence does not match service {:?} at index {expected_index}",
                    plan.plan.service.as_str()
                ));
            }
        }
    }
    Ok(())
}

fn materialize_source_checkout_serialized(
    runtime_root: PathBuf,
    source: &ComposeSourceDefinition,
) -> Result<(PathBuf, LocalFileLock), String> {
    let plan = plan_source_checkout(runtime_root, source).map_err(|error| error.to_string())?;
    let lock_path = source_checkout_lock_path(&plan)?;
    let owner = source_checkout_lock_owner();
    let mut lock = LocalFileLock::acquire(
        &lock_path,
        owner,
        LocalFileLockOptions {
            wait: true,
            wait_timeout: SOURCE_LOCK_WAIT_TIMEOUT,
            retry_interval: SOURCE_LOCK_RETRY_INTERVAL,
        },
    )
    .map_err(|error| format!("could not acquire source checkout lifetime lock: {error}"))?;

    match execute_source_checkout(&plan) {
        Ok(root) => Ok((root, lock)),
        Err(work) => match lock.release() {
            Ok(()) => Err(work.to_string()),
            Err(release) => Err(format!(
                "source checkout materialization failed: {work}; lifetime lock release also failed: {release}"
            )),
        },
    }
}

fn finish_with_source_lock(
    execution_result: Result<(), String>,
    source_lock: Option<LocalFileLock>,
) -> Result<(), String> {
    let Some(mut lock) = source_lock else {
        return execution_result;
    };
    let release_result = lock.release();
    match (execution_result, release_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(work), Ok(())) => Err(work),
        (Ok(()), Err(release)) => Err(format!(
            "compose execution completed but source checkout lifetime lock release failed: {release}"
        )),
        (Err(work), Err(release)) => Err(format!(
            "compose execution failed: {work}; source checkout lifetime lock release also failed: {release}"
        )),
    }
}

fn source_checkout_lock_path(plan: &SourceCheckoutPlan) -> Result<PathBuf, String> {
    let runtime = fs::canonicalize(&plan.runtime_root)
        .map_err(|error| format!("could not canonicalize source lock runtime root: {error}"))?;
    if runtime != plan.runtime_root {
        return Err("source lock runtime root changed during admission".to_string());
    }

    let ores = runtime.join(".ores");
    ensure_real_directory(&ores, ".ores source lock parent")?;
    let lock_root = ores.join("source-locks");
    ensure_real_directory(&lock_root, "source lock root")?;

    let checkout_identity = plan.checkout_root.to_string_lossy();
    let digest = Sha256::digest(checkout_identity.as_bytes());
    Ok(lock_root.join(format!("{digest:x}.lock")))
}

fn ensure_real_directory(path: &Path, label: &str) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(format!("{label} must be an unaliased directory"));
            }
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(format!("could not create {label}: {error}")),
            }
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| format!("could not inspect {label}: {error}"))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(format!("{label} must be an unaliased directory"));
            }
            Ok(())
        }
        Err(error) => Err(format!("could not inspect {label}: {error}")),
    }
}

fn source_checkout_lock_owner() -> String {
    let sequence = SOURCE_LOCK_OWNER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("ores-compose:{}:{nanos}:{sequence}", std::process::id())
}

async fn fail_with_shutdown(
    running: &mut [RunningService],
    shutdown_waves: &[Vec<RoutingLabel>],
    error: String,
) -> Result<(), String> {
    shutdown(running, shutdown_waves).await;
    Err(error)
}

async fn run_waves(
    project_name: &str,
    project_root: &Path,
    isolation_binary: Option<&Path>,
    waves: Vec<Vec<TrustedServiceProcessPlan>>,
    replica_plans: BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
    build_environments: BuildEnvironments,
    shutdown_waves: Vec<Vec<RoutingLabel>>,
) -> Result<(), String> {
    let mut running = Vec::<RunningService>::new();

    for (wave_index, wave) in waves.into_iter().enumerate() {
        for service in &wave {
            let build_environment = match build_environments.get(&service.plan.service) {
                Some(environment) => environment,
                None => {
                    return fail_with_shutdown(
                        &mut running,
                        &shutdown_waves,
                        format!(
                            "missing admitted build environment for service {:?}",
                            service.plan.service.as_str()
                        ),
                    )
                    .await;
                }
            };
            for build in &service.plan.build {
                if let Err(error) = service.validate_before_execution() {
                    return fail_with_shutdown(&mut running, &shutdown_waves, error.to_string())
                        .await;
                }
                if let Err(error) = run_build(
                    service.plan.service.as_str(),
                    project_root,
                    isolation_binary,
                    build,
                    build_environment,
                )
                .await
                {
                    return fail_with_shutdown(&mut running, &shutdown_waves, error).await;
                }
            }
        }

        let wave_start = running.len();
        for service in wave {
            let service_name = service.plan.service.as_str().to_owned();
            let replicas = match replica_plans.get(&service.plan.service) {
                Some(replicas) => replicas,
                None => {
                    return fail_with_shutdown(
                        &mut running,
                        &shutdown_waves,
                        format!(
                            "missing admitted replica runtime plan for service {:?}",
                            service_name
                        ),
                    )
                    .await;
                }
            };

            for replica in replicas {
                if let Err(error) = service.validate_before_execution() {
                    return fail_with_shutdown(&mut running, &shutdown_waves, error.to_string())
                        .await;
                }
                let (child, isolation_policy_path) = match spawn_service(
                    &service_name,
                    &replica.replica_id,
                    project_root,
                    isolation_binary,
                    &service.plan.start,
                    &replica.environment,
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        return fail_with_shutdown(&mut running, &shutdown_waves, error).await;
                    }
                };
                eprintln!(
                    "{{\"event\":\"service_started\",\"project\":{:?},\"service\":{:?},\"replica\":{:?},\"replica_index\":{},\"wave\":{wave_index}}}",
                    project_name,
                    service_name,
                    replica.replica_id,
                    replica.replica_index
                );
                running.push(RunningService {
                    service: service_name.clone(),
                    replica_id: replica.replica_id.clone(),
                    environment: replica.environment.clone(),
                    child,
                    healthcheck: service.plan.healthcheck.clone(),
                    working_directory: service.working_directory.clone(),
                    isolation_policy_path,
                });
            }
        }

        for service in &mut running[wave_start..] {
            if let Err(error) = wait_ready(service, project_root, isolation_binary).await {
                shutdown(&mut running, &shutdown_waves).await;
                return Err(error);
            }
            eprintln!(
                "{{\"event\":\"service_ready\",\"project\":{:?},\"service\":{:?},\"replica\":{:?},\"wave\":{wave_index}}}",
                project_name,
                service.service,
                service.replica_id
            );
        }
    }

    eprintln!(
        "{{\"event\":\"compose_ready\",\"project\":{:?},\"replicas\":{}}}",
        project_name,
        running.len()
    );

    let failure = loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                match result {
                    Ok(()) => break None,
                    Err(error) => break Some(format!("could not install Ctrl-C handler: {error}")),
                }
            }
            _ = sleep(Duration::from_millis(200)) => {
                let mut exited = None;
                for service in &mut running {
                    match service.child.try_wait() {
                        Ok(Some(status)) => {
                            exited = Some(format!(
                                "service {:?} replica {:?} exited while the compose project was running with status {status}",
                                service.service,
                                service.replica_id
                            ));
                            break;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            exited = Some(format!(
                                "could not inspect service {:?} replica {:?}: {error}",
                                service.service,
                                service.replica_id
                            ));
                            break;
                        }
                    }
                }
                if exited.is_some() {
                    break exited;
                }
            }
        }
    };

    shutdown(&mut running, &shutdown_waves).await;
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}

fn resolve_process_isolation_binary() -> Result<Option<PathBuf>, String> {
    let mode = env::var("ORES_COMPOSE_PROCESS_ISOLATION")
        .unwrap_or_else(|_| "auto".to_string())
        .to_ascii_lowercase();
    if mode == "off" {
        return Ok(None);
    }
    if mode != "auto" && mode != "required" {
        return Err(
            "ORES_COMPOSE_PROCESS_ISOLATION must be one of: auto, required, off".to_string(),
        );
    }
    if !cfg!(target_os = "macos") {
        return if mode == "required" {
            Err("ores-compose process isolation integration is currently implemented for macOS local development only".to_string())
        } else {
            Ok(None)
        };
    }

    let configured = env::var_os("ORES_PROC_ISOLATION_BIN").map(PathBuf::from);
    let resolved = match configured {
        Some(path) => Some(resolve_configured_process_isolation_binary(&path)?),
        None => find_trusted_process_isolation_binary(),
    };

    match (mode.as_str(), resolved) {
        (_, Some(path)) => Ok(Some(path)),
        ("required", None) => Err(
            "ORES_COMPOSE_PROCESS_ISOLATION=required but ores-proc-isolation was not found; install it or set ORES_PROC_ISOLATION_BIN to an absolute executable"
                .to_string(),
        ),
        _ => Ok(None),
    }
}

fn resolve_configured_process_isolation_binary(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(
            "ORES_PROC_ISOLATION_BIN must be an absolute path so helper selection cannot be influenced by PATH"
                .to_string(),
        );
    }
    resolve_executable_path(path, Path::new("/"))
        .map_err(|error| format!("invalid ORES_PROC_ISOLATION_BIN: {error}"))
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn canonicalize_trusted_helper(candidate: &Path, trusted_root: &Path) -> Option<PathBuf> {
    if !is_executable_file(candidate) {
        return None;
    }
    let canonical = fs::canonicalize(candidate).ok()?;
    let canonical_root = fs::canonicalize(trusted_root).ok()?;
    if canonical == canonical_root || canonical.starts_with(&canonical_root) {
        Some(canonical)
    } else {
        None
    }
}

fn find_trusted_process_isolation_binary() -> Option<PathBuf> {
    [
        (
            Path::new("/opt/homebrew/bin/ores-proc-isolation"),
            Path::new("/opt/homebrew"),
        ),
        (
            Path::new("/usr/local/bin/ores-proc-isolation"),
            Path::new("/usr/local"),
        ),
        (
            Path::new("/usr/bin/ores-proc-isolation"),
            Path::new("/usr"),
        ),
    ]
    .into_iter()
    .find_map(|(candidate, root)| canonicalize_trusted_helper(candidate, root))
}

fn find_executable_on_path(program: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    find_executable_on_search_path(program, &path)
}

fn find_executable_on_search_path(program: &str, path: &std::ffi::OsStr) -> Option<PathBuf> {
    env::split_paths(path)
        .map(|directory| directory.join(program))
        .find_map(|candidate| {
            if is_executable_file(&candidate) {
                fs::canonicalize(candidate).ok()
            } else {
                None
            }
        })
}

fn resolve_executable_path(program: &Path, working_directory: &Path) -> Result<PathBuf, String> {
    let candidate = if program.is_absolute() {
        program.to_path_buf()
    } else if program.components().count() > 1 {
        working_directory.join(program)
    } else {
        return find_executable_on_path(
            program
                .to_str()
                .ok_or_else(|| "process program is not valid UTF-8".to_string())?,
        )
        .ok_or_else(|| format!("could not resolve executable {:?} on PATH", program));
    };
    let canonical = fs::canonicalize(&candidate)
        .map_err(|error| format!("could not canonicalize executable {}: {error}", candidate.display()))?;
    if !is_executable_file(&canonical) {
        return Err(format!(
            "resolved executable is not an executable regular file: {}",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn tool_read_only_roots(
    project_root: &Path,
    executable: &Path,
    admitted_path: Option<&std::ffi::OsStr>,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let home = env::var_os("HOME").map(PathBuf::from);

    // Only the already-admitted PATH may expand the sandbox read surface.
    // Ambient PATH is not policy authority and must not grant filesystem reads.
    if let Some(path) = admitted_path {
        for directory in env::split_paths(path) {
            if let Ok(canonical) = fs::canonicalize(directory) {
                if approved_tool_path_root(&canonical, home.as_deref()) {
                    push_read_only_root(&mut roots, project_root, canonical);
                }
            }
        }
    }

    for prefix in ["/opt/homebrew", "/usr/local", "/nix/store"] {
        let prefix = Path::new(prefix);
        if executable.starts_with(prefix) && prefix.exists() {
            push_read_only_root(&mut roots, project_root, prefix.to_path_buf());
        }
    }

    if let Some(home) = home {
        for relative in [".rustup", ".nvm", ".volta", ".asdf"] {
            let root = home.join(relative);
            if executable.starts_with(&root) && root.exists() {
                push_read_only_root(&mut roots, project_root, root);
            }
        }
        for relative in [".cargo/bin", ".bun/bin"] {
            let root = home.join(relative);
            if executable.starts_with(&root) && root.exists() {
                push_read_only_root(&mut roots, project_root, root);
            }
        }
    }

    roots.sort();
    roots.dedup();
    roots
}

fn approved_tool_path_root(candidate: &Path, home: Option<&Path>) -> bool {
    const SYSTEM_ROOTS: &[&str] = &[
        "/bin",
        "/sbin",
        "/usr/bin",
        "/usr/sbin",
        "/opt/homebrew",
        "/usr/local",
        "/nix/store",
    ];
    if SYSTEM_ROOTS
        .iter()
        .any(|root| candidate == Path::new(root) || candidate.starts_with(root))
    {
        return true;
    }

    let Some(home) = home else {
        return false;
    };
    [
        ".cargo/bin",
        ".rustup/toolchains",
        ".nvm/versions",
        ".volta/bin",
        ".bun/bin",
        ".asdf/shims",
        ".asdf/installs",
    ]
    .iter()
    .map(|relative| home.join(relative))
    .any(|root| candidate == root || candidate.starts_with(root))
}

fn push_read_only_root(roots: &mut Vec<PathBuf>, project_root: &Path, candidate: PathBuf) {
    if candidate == Path::new("/")
        || candidate == project_root
        || candidate.starts_with(project_root)
        || project_root.starts_with(&candidate)
    {
        return;
    }
    if !roots
        .iter()
        .any(|existing| candidate.starts_with(existing) || existing.starts_with(&candidate))
    {
        roots.push(candidate);
    }
}

fn isolation_policy_path(sequence: u64, nonce: u128) -> PathBuf {
    Path::new(TRUSTED_ISOLATION_POLICY_ROOT).join(format!(
        "ores-compose-isolation-{}-{nonce}-{sequence}.yaml",
        std::process::id()
    ))
}

fn write_isolation_policy(
    project_root: &Path,
    spec: &ProcessCommandSpec,
    environment: &BTreeMap<String, String>,
) -> Result<(PathBuf, String), String> {
    let project_root = fs::canonicalize(project_root)
        .map_err(|error| format!("could not canonicalize isolation project root: {error}"))?;
    let working_directory = fs::canonicalize(&spec.working_dir)
        .map_err(|error| format!("could not canonicalize isolated working directory: {error}"))?;
    if working_directory != project_root && !working_directory.starts_with(&project_root) {
        return Err("isolated working directory escaped the trusted project root".to_string());
    }

    let program = Path::new(&spec.program);
    let executable = if program.components().count() == 1 {
        let admitted_path = environment.get("PATH").ok_or_else(|| {
            "isolated bare program requires an admitted PATH value".to_string()
        })?;
        find_executable_on_search_path(&spec.program, std::ffi::OsStr::new(admitted_path))
            .ok_or_else(|| {
                format!(
                    "could not resolve isolated executable {:?} on admitted PATH",
                    spec.program
                )
            })?
    } else {
        resolve_executable_path(program, &working_directory)?
    };
    let admitted_path = environment
        .get("PATH")
        .map(|path| std::ffi::OsStr::new(path));
    let read_only = tool_read_only_roots(&project_root, &executable, admitted_path);
    let mut yaml = String::new();
    yaml.push_str("version: 1\n");
    yaml.push_str("defaults:\n  group: local-compose\n");
    yaml.push_str("groups:\n  local-compose:\n    filesystem:\n");
    if read_only.is_empty() {
        yaml.push_str("      read_only: []\n");
    } else {
        yaml.push_str("      read_only:\n");
        for path in read_only {
            yaml.push_str("        - ");
            yaml.push_str(
                &serde_json::to_string(&path.to_string_lossy()).map_err(|error| error.to_string())?,
            );
            yaml.push('\n');
        }
    }
    yaml.push_str("      read_write:\n        - ");
    yaml.push_str(&serde_json::to_string(&project_root.to_string_lossy()).map_err(|e| e.to_string())?);
    yaml.push('\n');
    yaml.push_str(
        "    network:\n      mode: local\n      deny_loopback: false\n      deny_private_networks: false\n",
    );
    yaml.push_str("    limits:\n      max_open_files: 1024\n      cpu_seconds: 3600\n");
    yaml.push_str("processes:\n  target:\n    group: local-compose\n    command:\n      - ");
    yaml.push_str(&serde_json::to_string(&executable.to_string_lossy()).map_err(|e| e.to_string())?);
    yaml.push('\n');
    for arg in &spec.args {
        yaml.push_str("      - ");
        yaml.push_str(&serde_json::to_string(arg).map_err(|e| e.to_string())?);
        yaml.push('\n');
    }
    yaml.push_str("    working_directory: ");
    yaml.push_str(
        &serde_json::to_string(&working_directory.to_string_lossy()).map_err(|e| e.to_string())?,
    );
    yaml.push('\n');
    if !environment.is_empty() {
        yaml.push_str("    environment:\n");
        for key in environment.keys() {
            yaml.push_str("      ");
            yaml.push_str(&serde_json::to_string(key).map_err(|e| e.to_string())?);
            yaml.push_str(": \"\"\n");
        }
    }
    let policy_sha256 = format!("{:x}", Sha256::digest(yaml.as_bytes()));
    for _ in 0..32 {
        let sequence = ISOLATION_POLICY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let path = isolation_policy_path(sequence, nonce);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!("could not create private isolation policy: {error}"));
            }
        };
        if let Err(error) = file.write_all(yaml.as_bytes()).and_then(|_| file.sync_all()) {
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(format!("could not write private isolation policy: {error}"));
        }
        return Ok((path, policy_sha256));
    }
    Err("could not create private isolation policy after repeated name collisions".to_string())
}

fn isolated_command(
    isolation_binary: &Path,
    policy_path: &Path,
    policy_sha256: &str,
) -> Command {
    let mut command = Command::new(isolation_binary);
    command
        .arg("--target-env-stdin")
        .arg("--config-sha256")
        .arg(policy_sha256)
        .arg("run")
        .arg("target")
        .arg("--config")
        .arg(policy_path);
    command
}

fn encode_isolation_environment(
    environment: &BTreeMap<String, String>,
) -> Result<Vec<u8>, String> {
    if environment.len() > 512 {
        return Err("isolated environment exceeds 512-key stdin boundary".to_string());
    }
    let encoded = serde_json::to_vec(environment)
        .map_err(|error| format!("could not encode isolated environment: {error}"))?;
    if encoded.len() > 256 * 1024 {
        return Err("isolated environment exceeds 256 KiB stdin boundary".to_string());
    }
    Ok(encoded)
}

async fn stream_isolation_environment(
    child: &mut Child,
    encoded_environment: &[u8],
) -> Result<(), String> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "isolated launcher stdin pipe was unavailable".to_string())?;
    stdin
        .write_all(encoded_environment)
        .await
        .map_err(|error| format!("could not stream isolated environment: {error}"))?;
    stdin
        .shutdown()
        .await
        .map_err(|error| format!("could not close isolated environment stream: {error}"))
}

fn remove_isolation_policy(path: Option<PathBuf>) {
    if let Some(path) = path {
        let _ = fs::remove_file(path);
    }
}


async fn run_build(
    service: &str,
    project_root: &Path,
    isolation_binary: Option<&Path>,
    spec: &ProcessCommandSpec,
    environment: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut policy_path = None;
    let status = if let Some(binary) = isolation_binary {
        let encoded_environment = encode_isolation_environment(environment)?;
        let (path, policy_sha256) = write_isolation_policy(project_root, spec, environment)?;
        let mut command = isolated_command(binary, &path, &policy_sha256);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        policy_path = Some(path);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                remove_isolation_policy(policy_path.take());
                return Err(format!(
                    "could not start isolated build for service {service:?}: {error}"
                ));
            }
        };
        if let Err(error) = stream_isolation_environment(&mut child, &encoded_environment).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            remove_isolation_policy(policy_path.take());
            return Err(error);
        }
        match child.wait().await {
            Ok(status) => status,
            Err(error) => {
                remove_isolation_policy(policy_path.take());
                return Err(format!(
                    "could not wait for isolated build for service {service:?}: {error}"
                ));
            }
        }
    } else {
        Command::new(&spec.program)
            .args(&spec.args)
            .current_dir(&spec.working_dir)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .map_err(|error| format!("could not start build for service {service:?}: {error}"))?
    };
    remove_isolation_policy(policy_path);
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "build for service {service:?} failed with status {status}"
        ))
    }
}

async fn spawn_service(
    service: &str,
    replica_id: &str,
    project_root: &Path,
    isolation_binary: Option<&Path>,
    spec: &ProcessCommandSpec,
    environment: &BTreeMap<String, String>,
) -> Result<(Child, Option<PathBuf>), String> {
    let mut isolation_policy_path = None;
    let isolated = isolation_binary.is_some();
    let encoded_environment = if isolated {
        Some(encode_isolation_environment(environment)?)
    } else {
        None
    };
    let mut command = if let Some(binary) = isolation_binary {
        let (policy, policy_sha256) = write_isolation_policy(project_root, spec, environment)?;
        let command = isolated_command(binary, &policy, &policy_sha256);
        isolation_policy_path = Some(policy);
        command
    } else {
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .current_dir(&spec.working_dir)
            .env_clear()
            .envs(environment);
        command
    };
    command
        .stdin(if isolated { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| {
        remove_isolation_policy(isolation_policy_path.take());
        format!(
            "could not start service {service:?} replica {replica_id:?}: {error}"
        )
    })?;
    if let Some(encoded_environment) = encoded_environment {
        if let Err(error) = stream_isolation_environment(&mut child, &encoded_environment).await {
            let _ = child.start_kill();
            let _ = child.wait().await;
            remove_isolation_policy(isolation_policy_path.take());
            return Err(format!(
                "could not stream environment to service {service:?} replica {replica_id:?}: {error}"
            ));
        }
    }
    Ok((child, isolation_policy_path))
}

async fn wait_ready(
    service: &mut RunningService,
    project_root: &Path,
    isolation_binary: Option<&Path>,
) -> Result<(), String> {
    let Some(healthcheck) = service.healthcheck.clone() else {
        sleep(Duration::from_millis(200)).await;
        return match service.child.try_wait() {
            Ok(None) => Ok(()),
            Ok(Some(status)) => Err(format!(
                "service {:?} replica {:?} exited before the readiness boundary with status {status}",
                service.service,
                service.replica_id
            )),
            Err(error) => Err(format!(
                "could not inspect service {:?} replica {:?} during readiness: {error}",
                service.service,
                service.replica_id
            )),
        };
    };

    let attempts = healthcheck.retries.max(1);
    for attempt in 1..=attempts {
        if let Some(status) = service.child.try_wait().map_err(|error| {
            format!(
                "could not inspect service {:?} replica {:?}: {error}",
                service.service, service.replica_id
            )
        })? {
            return Err(format!(
                "service {:?} replica {:?} exited before healthcheck success with status {status}",
                service.service, service.replica_id
            ));
        }

        service
            .working_directory
            .validate_current()
            .map_err(|error| {
                format!(
                    "trusted working directory changed before healthcheck for service {:?} replica {:?}: {error}",
                    service.service, service.replica_id
                )
            })?;
        if run_healthcheck(
            project_root,
            isolation_binary,
            &healthcheck,
            &service.environment,
        )
        .await
        {
            return Ok(());
        }
        if attempt < attempts {
            sleep(Duration::from_millis(healthcheck.interval_ms)).await;
        }
    }

    Err(format!(
        "service {:?} replica {:?} did not pass its healthcheck after {attempts} attempts",
        service.service, service.replica_id
    ))
}

async fn run_healthcheck(
    project_root: &Path,
    isolation_binary: Option<&Path>,
    healthcheck: &HealthcheckProcessPlan,
    environment: &BTreeMap<String, String>,
) -> bool {
    let command_spec = &healthcheck.command;
    let mut policy_path = None;
    let isolated = isolation_binary.is_some();
    let encoded_environment = if isolated {
        match encode_isolation_environment(environment) {
            Ok(encoded) => Some(encoded),
            Err(_) => return false,
        }
    } else {
        None
    };
    let mut command = if let Some(binary) = isolation_binary {
        let (policy, policy_sha256) = match write_isolation_policy(project_root, command_spec, environment) {
            Ok(policy) => policy,
            Err(_) => return false,
        };
        let command = isolated_command(binary, &policy, &policy_sha256);
        policy_path = Some(policy);
        command
    } else {
        let mut command = Command::new(&command_spec.program);
        command
            .args(&command_spec.args)
            .current_dir(&command_spec.working_dir)
            .env_clear()
            .envs(environment);
        command
    };
    command
        .stdin(if isolated { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            remove_isolation_policy(policy_path);
            return false;
        }
    };
    if let Some(encoded_environment) = encoded_environment {
        if stream_isolation_environment(&mut child, &encoded_environment)
            .await
            .is_err()
        {
            let _ = child.start_kill();
            let _ = child.wait().await;
            remove_isolation_policy(policy_path);
            return false;
        }
    }

    let result = match timeout(
        Duration::from_millis(healthcheck.timeout_ms),
        child.wait(),
    )
    .await
    {
        Ok(Ok(status)) => status.success(),
        Ok(Err(_)) | Err(_) => false,
    };
    if !result {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    remove_isolation_policy(policy_path);
    result
}

async fn shutdown(running: &mut [RunningService], shutdown_waves: &[Vec<RoutingLabel>]) {
    for (wave_index, wave) in shutdown_waves.iter().enumerate() {
        for target in wave {
            for service in running
                .iter_mut()
                .filter(|service| service.service == target.as_str())
            {
                let _ = service.child.start_kill();
            }
        }

        for target in wave {
            for service in running
                .iter_mut()
                .filter(|service| service.service == target.as_str())
            {
                let _ = timeout(Duration::from_secs(3), service.child.wait()).await;
                remove_isolation_policy(service.isolation_policy_path.take());
                eprintln!(
                    "{{\"event\":\"service_stopped\",\"service\":{:?},\"replica\":{:?},\"shutdown_wave\":{wave_index}}}",
                    service.service,
                    service.replica_id
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ores_compose::{parse_compose_yaml, plan_project_process_waves_trusted};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = TEST_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "ores-compose-up-test-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::canonicalize(root).unwrap()
    }

    fn admitted_plans(
        root: &Path,
        input: &str,
    ) -> (
        ComposeProjectDefinition,
        Vec<Vec<TrustedServiceProcessPlan>>,
        BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
    ) {
        let project = parse_compose_yaml(input).unwrap();
        let policies = parse_compose_replica_runtime_policies(input, &project).unwrap();
        let replicas = plan_project_replicas(&project, &policies).unwrap();
        let waves = plan_project_process_waves_trusted(root, &project).unwrap();
        (project, waves, replicas)
    }

    fn admit_environments(
        input: &str,
        project: &ComposeProjectDefinition,
        replicas: &mut BTreeMap<RoutingLabel, Vec<ReplicaRuntimePlan>>,
    ) -> BuildEnvironments {
        let policies = parse_compose_service_environment_policies(input, project).unwrap();
        admit_service_environments(project, &policies, replicas).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn trusted_up_loader_rejects_symlink_config_before_parsing_or_network() {
        use std::os::unix::fs::symlink;
        let root = temp_root();
        let real = root.join("real.yaml");
        let config = root.join(".ores-compose.yaml");
        fs::write(
            &real,
            "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    command: [\"api\"]\n",
        )
        .unwrap();
        symlink(&real, &config).unwrap();
        let error = load_trusted_up_config(&config).expect_err("symlink must fail closed");
        assert!(error.contains("must not be a symlink"));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn trusted_helper_symlink_cannot_escape_trusted_root() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = temp_root();
        let trusted = root.join("trusted");
        let outside = root.join("outside");
        fs::create_dir(&trusted).unwrap();
        fs::create_dir(&outside).unwrap();
        let target = outside.join("ores-proc-isolation");
        fs::write(&target, b"placeholder").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
        let link = trusted.join("ores-proc-isolation");
        symlink(&target, &link).unwrap();

        assert!(canonicalize_trusted_helper(&link, &trusted).is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sandbox_read_roots_do_not_expand_from_ambient_path() {
        let root = temp_root();
        let roots = tool_read_only_roots(&root, Path::new("/bin/sh"), None);
        assert!(
            roots.is_empty(),
            "ambient PATH must not grant sandbox read roots: {roots:?}"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn isolation_policy_path_ignores_ambient_temp_directory() {
        let path = isolation_policy_path(7, 11);
        assert_eq!(path.parent(), Some(Path::new(TRUSTED_ISOLATION_POLICY_ROOT)));
        assert!(path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("ores-compose-isolation-")));
    }

    #[test]
    fn configured_process_isolation_binary_rejects_relative_override() {
        let error = resolve_configured_process_isolation_binary(Path::new("ores-proc-isolation"))
            .expect_err("relative isolation helper override must fail closed");
        assert!(error.contains("must be an absolute path"));
    }

    #[cfg(unix)]
    #[test]
    fn executable_resolution_rejects_non_executable_regular_file() {
        let root = temp_root();
        let candidate = root.join("not-executable");
        fs::write(&candidate, b"placeholder").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o600)).unwrap();

        let error = resolve_executable_path(&candidate, &root)
            .expect_err("non-executable regular files must fail admission");
        assert!(error.contains("not an executable regular file"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn isolated_bare_program_resolves_only_from_admitted_path() {
        let root = temp_root();
        let admitted_bin = root.join("admitted-bin");
        fs::create_dir(&admitted_bin).unwrap();
        let executable = admitted_bin.join("demo-tool");
        fs::write(&executable, b"placeholder").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        }

        let spec = ProcessCommandSpec {
            program: "demo-tool".to_string(),
            args: vec![],
            working_dir: root.clone(),
        };
        let mut environment = BTreeMap::new();
        environment.insert(
            "PATH".to_string(),
            admitted_bin.to_string_lossy().into_owned(),
        );

        let (policy, policy_sha256) = write_isolation_policy(&root, &spec, &environment).unwrap();
        let contents = fs::read_to_string(&policy).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(contents.as_bytes())), policy_sha256);
        let canonical = fs::canonicalize(&executable).unwrap();
        assert!(contents.contains(&canonical.to_string_lossy().to_string()));
        remove_isolation_policy(Some(policy));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_accepts_single_replica_host_services() {
        let root = temp_root();
        let (project, waves, replicas) = admitted_plans(
            &root,
            "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n",
        );
        assert!(preflight_project_execution(&project, &replicas).is_ok());
        assert!(preflight(&waves, &replicas).is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_accepts_multi_replica_host_service_with_canonical_identity_env() {
        let root = temp_root();
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n    replicas: 2\n    host_bind: { env: API_BIND, host: 127.0.0.1, start_port: 18080 }\n";
        let (project, waves, replicas) = admitted_plans(&root, input);
        assert!(preflight_project_execution(&project, &replicas).is_ok());
        assert!(preflight(&waves, &replicas).is_ok());
        let api = replicas.values().next().unwrap();
        assert_eq!(api.len(), 2);
        assert_eq!(api[0].replica_id, "api-r1");
        assert_eq!(api[1].environment["API_BIND"], "127.0.0.1:18081");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn environment_admission_merges_authored_and_runtime_values() {
        let root = temp_root();
        let input = r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
services:
  api:
    runtime: host
    command: ["/bin/true"]
    environment:
      API_MODE: "local"
    replicas: 2
    host_bind: { env: API_BIND, host: 127.0.0.1, start_port: 18080 }
"#;
        let (project, _, mut replicas) = admitted_plans(&root, input);
        let build = admit_environments(input, &project, &mut replicas);
        let service = RoutingLabel::new("api").unwrap();
        assert_eq!(build[&service]["API_MODE"], "local");
        assert_eq!(build[&service][ORES_COMPOSE_SERVICE_ENV], "api");
        assert!(!build[&service].contains_key("API_BIND"));
        assert_eq!(replicas[&service][1].environment["API_MODE"], "local");
        assert_eq!(
            replicas[&service][1].environment["API_BIND"],
            "127.0.0.1:18081"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_secret_fails_before_source_materialization() {
        let root = temp_root();
        let config = root.join(".ores-compose.yaml");
        fs::write(
            &config,
            r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
source:
  repository: https://github.com/example/never-fetch.git
  commit: 0123456789abcdef0123456789abcdef01234567
  checkout_dir: .ores/sources/test
services:
  api:
    command: ["/bin/true"]
    secret_env:
      API_TOKEN: ORES_TEST_MISSING_SECRET_9D443A8C
"#,
        )
        .unwrap();
        let error = run(Some(config)).expect_err("missing secret must fail before Git");
        assert!(
            error.contains("required secret environment source"),
            "{error}"
        );
        assert!(
            !root.join(".ores").exists(),
            "source side effects must not begin"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_environment_collision_fails_before_source_materialization() {
        let root = temp_root();
        let config = root.join(".ores-compose.yaml");
        fs::write(
            &config,
            r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
source:
  repository: https://github.com/example/never-fetch.git
  commit: 0123456789abcdef0123456789abcdef01234567
  checkout_dir: .ores/sources/test
services:
  api:
    command: ["/bin/true"]
    environment:
      API_BIND: "attacker-controlled"
    host_bind: { env: API_BIND, host: 127.0.0.1, start_port: 18080 }
"#,
        )
        .unwrap();
        let error = run(Some(config)).expect_err("runtime-owned collision must fail before Git");
        assert!(error.contains("runtime-owned key"), "{error}");
        assert!(
            !root.join(".ores").exists(),
            "source side effects must not begin"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bare_program_requires_explicit_path_before_source_materialization() {
        let root = temp_root();
        let config = root.join(".ores-compose.yaml");
        fs::write(
            &config,
            r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
source:
  repository: https://github.com/example/never-fetch.git
  commit: 0123456789abcdef0123456789abcdef01234567
  checkout_dir: .ores/sources/test
services:
  api:
    command: ["sh", "-c", "true"]
"#,
        )
        .unwrap();
        let error = run(Some(config)).expect_err("bare program must require admitted PATH");
        assert!(
            error.contains("PATH must be explicitly listed in inherit_env"),
            "{error}"
        );
        assert!(
            !root.join(".ores").exists(),
            "source side effects must not begin"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_rejects_non_host_runtime_before_materialization() {
        let root = temp_root();
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: docker\n    command: [\"api\"]\n";
        let project = parse_compose_yaml(input).unwrap();
        let policies = parse_compose_replica_runtime_policies(input, &project).unwrap();
        let replicas = plan_project_replicas(&project, &policies).unwrap();
        assert!(preflight_project_execution(&project, &replicas)
            .unwrap_err()
            .contains("host-process services only"));
        assert!(!root.join(".ores").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_rejects_missing_replica_runtime_evidence() {
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n";
        let project = parse_compose_yaml(input).unwrap();
        assert!(preflight_project_execution(&project, &BTreeMap::new())
            .unwrap_err()
            .contains("missing admitted replica runtime plan"));
    }

    #[test]
    fn early_preflight_rejects_replica_count_mismatch() {
        let root = temp_root();
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n    replicas: 2\n";
        let (project, _, mut replicas) = admitted_plans(&root, input);
        replicas.values_mut().next().unwrap().pop();
        assert!(preflight_project_execution(&project, &replicas)
            .unwrap_err()
            .contains("runtime planning produced"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_rejects_replica_service_identity_mismatch() {
        let root = temp_root();
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n";
        let (project, _, mut replicas) = admitted_plans(&root, input);
        replicas.values_mut().next().unwrap()[0].service = RoutingLabel::new("other").unwrap();
        assert!(preflight_project_execution(&project, &replicas)
            .unwrap_err()
            .contains("replica runtime evidence does not match"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn early_preflight_rejects_replica_index_mismatch() {
        let root = temp_root();
        let input = "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n";
        let (project, _, mut replicas) = admitted_plans(&root, input);
        replicas.values_mut().next().unwrap()[0].replica_index = 1;
        assert!(preflight_project_execution(&project, &replicas)
            .unwrap_err()
            .contains("replica runtime evidence does not match"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_lock_is_keyed_by_checkout_path_not_repository_or_commit() {
        let root = temp_root();
        let source_a = ComposeSourceDefinition {
            repository: "https://github.com/example/a.git".to_string(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            checkout_dir: PathBuf::from(".ores/sources/shared"),
        };
        let source_b = ComposeSourceDefinition {
            repository: "https://github.com/example/b.git".to_string(),
            commit: "fedcba9876543210fedcba9876543210fedcba98".to_string(),
            checkout_dir: PathBuf::from(".ores/sources/shared"),
        };
        let plan_a = plan_source_checkout(root.clone(), &source_a).unwrap();
        let plan_b = plan_source_checkout(root.clone(), &source_b).unwrap();
        assert_eq!(
            source_checkout_lock_path(&plan_a).unwrap(),
            source_checkout_lock_path(&plan_b).unwrap()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn different_checkout_paths_get_different_source_locks() {
        let root = temp_root();
        let base = ComposeSourceDefinition {
            repository: "https://github.com/example/a.git".to_string(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            checkout_dir: PathBuf::from(".ores/sources/a"),
        };
        let mut other = base.clone();
        other.checkout_dir = PathBuf::from(".ores/sources/b");
        let plan_a = plan_source_checkout(root.clone(), &base).unwrap();
        let plan_b = plan_source_checkout(root.clone(), &other).unwrap();
        assert_ne!(
            source_checkout_lock_path(&plan_a).unwrap(),
            source_checkout_lock_path(&plan_b).unwrap()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_lock_contention_is_fail_closed() {
        let root = temp_root();
        let source = ComposeSourceDefinition {
            repository: "https://github.com/example/a.git".to_string(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            checkout_dir: PathBuf::from(".ores/sources/a"),
        };
        let plan = plan_source_checkout(root.clone(), &source).unwrap();
        let lock_path = source_checkout_lock_path(&plan).unwrap();
        let mut first = LocalFileLock::try_acquire(&lock_path, "first")
            .unwrap()
            .unwrap();
        assert!(LocalFileLock::try_acquire(&lock_path, "second")
            .unwrap()
            .is_none());
        first.release().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_source_lock_root_is_rejected() {
        use std::os::unix::fs::symlink;
        let root = temp_root();
        let outside = temp_root();
        fs::create_dir(root.join(".ores")).unwrap();
        symlink(&outside, root.join(".ores/source-locks")).unwrap();
        let source = ComposeSourceDefinition {
            repository: "https://github.com/example/a.git".to_string(),
            commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            checkout_dir: PathBuf::from(".ores/sources/a"),
        };
        let plan = plan_source_checkout(root.clone(), &source).unwrap();
        assert!(source_checkout_lock_path(&plan)
            .unwrap_err()
            .contains("unalias"));
        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(outside);
    }

    #[test]
    fn source_lock_owner_tokens_are_unique_within_process() {
        assert_ne!(source_checkout_lock_owner(), source_checkout_lock_owner());
    }

    #[test]
    fn preflight_rejects_missing_replica_runtime_evidence_after_materialization_too() {
        let root = temp_root();
        let (_, waves, _) = admitted_plans(
            &root,
            "schema_version: ores.compose.v1\nproject: test\nallow_lazy_start: false\nservices:\n  api:\n    runtime: host\n    command: [\"api\"]\n",
        );
        assert!(preflight(&waves, &BTreeMap::new())
            .unwrap_err()
            .contains("missing admitted replica runtime plan"));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn build_service_and_healthcheck_clear_ambient_environment() {
        assert!(
            env::var_os("HOME").is_some(),
            "test requires an ambient HOME"
        );
        let root = temp_root();
        let environment = BTreeMap::from([("EXPLICIT".to_string(), "yes".to_string())]);

        let build = ProcessCommandSpec {
            program: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "test -z \"${HOME+x}\" && test \"$EXPLICIT\" = yes".to_string(),
            ],
            working_dir: root.clone(),
        };
        run_build("api", &root, None, &build, &environment)
            .await
            .expect("build environment must be isolated");

        let service = ProcessCommandSpec {
            program: "/bin/sh".to_string(),
            args: vec![
                "-c".to_string(),
                "test -z \"${HOME+x}\" && test \"$EXPLICIT\" = yes && exec /bin/sleep 30"
                    .to_string(),
            ],
            working_dir: root.clone(),
        };
        let (mut child, policy_path) =
            spawn_service("api", "api-r1", &root, None, &service, &environment)
                .await
                .expect("service environment must be isolated");
        assert!(policy_path.is_none());
        sleep(Duration::from_millis(100)).await;
        assert!(
            child.try_wait().unwrap().is_none(),
            "service should still be alive"
        );
        let _ = child.start_kill();
        let _ = timeout(Duration::from_secs(3), child.wait()).await;

        let healthcheck = HealthcheckProcessPlan {
            command: ProcessCommandSpec {
                program: "/bin/sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "test -z \"${HOME+x}\" && test \"$EXPLICIT\" = yes".to_string(),
                ],
                working_dir: root.clone(),
            },
            interval_ms: 1,
            timeout_ms: 1_000,
            retries: 1,
        };
        assert!(run_healthcheck(&root, None, &healthcheck, &environment).await);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn revalidates_working_directory_between_build_commands() {
        let root = temp_root();
        fs::create_dir_all(root.join("services/api")).unwrap();
        let input = r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
services:
  api:
    runtime: host
    command: ["/bin/true"]
    working_dir: services/api
    build:
      - ["/bin/sh", "-c", "cd .. && mv api api-old && mkdir api"]
      - ["/bin/sh", "-c", "printf should-not-run > ../second-build-ran"]
"#;
        let (project, waves, mut replicas) = admitted_plans(&root, input);
        let build_environments = admit_environments(input, &project, &mut replicas);
        let shutdown_waves = plan_project_shutdown_waves(&project).unwrap();
        let error = run_waves(
            "test",
            &root,
            None,
            waves,
            replicas,
            build_environments,
            shutdown_waves,
        )
        .await
            .expect_err("replacement must fence the second build");
        assert!(
            error.contains("identity changed"),
            "unexpected error: {error}"
        );
        assert!(!root.join("services/second-build-ran").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn revalidates_working_directory_before_replica_spawn() {
        let root = temp_root();
        fs::create_dir_all(root.join("services/api")).unwrap();
        let input = r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
services:
  api:
    runtime: host
    command: ["/bin/sh", "-c", "printf should-not-run > ../service-started"]
    working_dir: services/api
    build:
      - ["/bin/sh", "-c", "cd .. && mv api api-old && mkdir api"]
"#;
        let (project, waves, mut replicas) = admitted_plans(&root, input);
        let build_environments = admit_environments(input, &project, &mut replicas);
        let shutdown_waves = plan_project_shutdown_waves(&project).unwrap();
        let error = run_waves(
            "test",
            &root,
            None,
            waves,
            replicas,
            build_environments,
            shutdown_waves,
        )
            .await
            .expect_err("replacement must fence replica spawn");
        assert!(
            error.contains("identity changed"),
            "unexpected error: {error}"
        );
        assert!(!root.join("services/service-started").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn revalidates_working_directory_before_healthcheck_spawn() {
        let root = temp_root();
        fs::create_dir_all(root.join("services/api")).unwrap();
        let input = r#"
schema_version: ores.compose.v1
project: test
allow_lazy_start: false
services:
  api:
    runtime: host
    command: ["/bin/sleep", "30"]
    working_dir: services/api
    healthcheck:
      command: ["/bin/sh", "-c", "printf should-not-run > ../healthcheck-ran"]
      interval_ms: 100
      timeout_ms: 100
      retries: 1
"#;
        let (project, mut waves, mut replicas) = admitted_plans(&root, input);
        let _ = admit_environments(input, &project, &mut replicas);
        let service = waves.remove(0).remove(0);
        let replica = replicas
            .get(&service.plan.service)
            .and_then(|plans| plans.first())
            .expect("replica plan")
            .clone();
        let service_name = service.plan.service.as_str().to_owned();
        service.validate_before_execution().unwrap();
        let (child, isolation_policy_path) = spawn_service(
            &service_name,
            &replica.replica_id,
            &root,
            None,
            &service.plan.start,
            &replica.environment,
        )
        .await
        .unwrap();
        let healthcheck = service.plan.healthcheck.clone();
        let working_directory = service.working_directory;

        fs::rename(root.join("services/api"), root.join("services/api-old")).unwrap();
        fs::create_dir(root.join("services/api")).unwrap();

        let mut running = RunningService {
            service: service_name,
            replica_id: replica.replica_id,
            environment: replica.environment,
            child,
            healthcheck,
            working_directory,
            isolation_policy_path,
        };
        let error = wait_ready(&mut running, &root, None)
            .await
            .expect_err("replacement must fence healthcheck spawn");
        assert!(
            error.contains("identity changed"),
            "unexpected error: {error}"
        );
        assert!(!root.join("services/healthcheck-ran").exists());
        let _ = running.child.start_kill();
        let _ = timeout(Duration::from_secs(3), running.child.wait()).await;
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn generated_isolation_policy_is_private_same_user_local_profile() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root();
        let work = root.join("service");
        fs::create_dir(&work).unwrap();

        let executable = root.join("worker");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).unwrap();

        let spec = ProcessCommandSpec {
            program: executable.to_string_lossy().into_owned(),
            args: vec!["--example".to_string()],
            working_dir: work,
        };
        let mut environment = BTreeMap::new();
        environment.insert("Example_Key".to_string(), "secret-value".to_string());

        let (policy, policy_sha256) = write_isolation_policy(&root, &spec, &environment).unwrap();
        let metadata = fs::metadata(&policy).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        let contents = fs::read_to_string(&policy).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(contents.as_bytes())), policy_sha256);
        assert!(contents.contains("mode: local"));
        assert!(contents.contains("deny_loopback: false"));
        assert!(contents.contains("deny_private_networks: false"));
        assert!(contents.contains("read_write:"));
        assert!(contents.contains(root.to_string_lossy().as_ref()));
        assert!(contents.contains("working_directory:"));
        assert!(!contents.contains("secret-value"));
        assert!(contents.contains("Example_Key"));
        assert!(contents.contains("environment:"));
        assert!(contents.contains(r#""Example_Key": """#));
        assert!(!contents.contains("sudo"));
        assert!(!contents.contains("uid:"));
        assert!(!contents.contains("gid:"));
        assert!(!contents.contains("user:"));

        remove_isolation_policy(Some(policy));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn isolation_environment_encoding_enforces_key_and_byte_bounds() {
        let too_many = (0..513)
            .map(|index| (format!("KEY_{index}"), "v".to_string()))
            .collect::<BTreeMap<_, _>>();
        let error = encode_isolation_environment(&too_many).expect_err("513 keys must fail");
        assert!(error.contains("512-key"));

        let oversized = BTreeMap::from([(
            "PAYLOAD".to_string(),
            "x".repeat(256 * 1024),
        )]);
        let error =
            encode_isolation_environment(&oversized).expect_err("oversized payload must fail");
        assert!(error.contains("256 KiB"));
    }

    #[test]
    fn isolation_environment_encoding_preserves_values_only_in_memory() {
        let environment = BTreeMap::from([
            ("MODE".to_string(), "worker".to_string()),
            ("API_TOKEN".to_string(), "secret-value".to_string()),
        ]);
        let encoded = encode_isolation_environment(&environment).expect("encode");
        let decoded: BTreeMap<String, String> =
            serde_json::from_slice(&encoded).expect("decode");
        assert_eq!(decoded, environment);
    }

    #[test]
    fn read_only_tool_roots_never_expand_to_project_parent_or_host_root() {
        let root = PathBuf::from("/Users/example/project");
        let mut roots = Vec::new();
        push_read_only_root(&mut roots, &root, PathBuf::from("/"));
        push_read_only_root(&mut roots, &root, PathBuf::from("/Users"));
        push_read_only_root(&mut roots, &root, root.join("bin"));
        push_read_only_root(&mut roots, &root, PathBuf::from("/opt/homebrew/bin"));
        assert_eq!(roots, vec![PathBuf::from("/opt/homebrew/bin")]);
    }

    #[test]
    fn path_grants_reject_broad_home_entries_but_allow_known_tool_bins() {
        let home = Path::new("/Users/example");
        assert!(!approved_tool_path_root(home, Some(home)));
        assert!(!approved_tool_path_root(
            Path::new("/Users/example/bin"),
            Some(home)
        ));
        assert!(approved_tool_path_root(
            Path::new("/Users/example/.cargo/bin"),
            Some(home)
        ));
        assert!(approved_tool_path_root(
            Path::new("/Users/example/.nvm/versions/node/v22/bin"),
            Some(home)
        ));
        assert!(approved_tool_path_root(
            Path::new("/opt/homebrew/bin"),
            Some(home)
        ));
    }


}
