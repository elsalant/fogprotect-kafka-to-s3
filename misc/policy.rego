package dataapi.authz

rule[{"action": {"name":"RedactColumn", "columns": column_names, "intent": input.context.intent}, "policy": description}] {
  description := "Redact columns tagged as PII in datasets tagged with healthcare = true"
  input.resource.metadata.tags.sm
  column_names := [input.resource.metadata.columns[i].name | input.resource.metadata.columns[i].tags.PII]
  count(column_names) > 0
}

rule[{"action": {"name":"BlockResource", "columns": column_names}, "policy": description}] {
  description := "Blocks whole resource"
  input.resource.metadata.tags.sm
  column_names := [input.resource.metadata.columns[i].name | input.resource.metadata.columns[i].tags.blocked]
  count(column_names) > 0
}
