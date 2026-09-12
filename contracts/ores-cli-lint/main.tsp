// Independently authored TypeSpec authority for the ores-cli lint contract.
// The emitted JSON Schema B is disposable comparison evidence only.
import "@typespec/json-schema";
using TypeSpec.JsonSchema;

namespace OresCliLint;

@id("AuthorityRole")
enum AuthorityRole {
  typespec: "typespec",
  jsonSchema: "json-schema",
}

@id("WorkflowBinding")
model WorkflowBinding {
  typespec: string;
  schema: string;
  generatedSchema: string;
  contractIr: string;
  report: string;
  outputDir: string;
  authority: AuthorityRole;
}
