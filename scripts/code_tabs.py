def code_tabs(
    sql: str,
    yaml: str,
    python_decl: str,
    python_api: str,
    *,
    title: str = "",
) -> str:
    """Return MkDocs Material content tabs Markdown block.

    Tab order is always: SQL, YAML, Python, Rivet API.

    - **SQL**: `.sql` files with `-- rivet:` annotations.
    - **YAML**: `.yaml` declaration files.
    - **Python**: A `.py` file placed in `joints/` (or `sources/`,
      `sinks/`) that uses `# rivet:` comment annotations and defines a
      `def transform(material: Material) -> pa.Table` function for Python
      joint types. For sources/sinks (catalog-based), show a note that
      Python declaration files are not applicable.
    - **Rivet API**: Constructing `Joint(...)`, `Assembly(...)`, `Catalog(...)`,
      `ComputeEngine(...)` directly using `rivet_core.models`. This is the
      programmatic way to build a pipeline without files.
    """
    prefix = f'/// tab | {title}\n' if title else ""
    suffix = "///" if title else ""

    def tab(label: str, lang: str, code: str) -> str:
        indented = "\n".join(f"    {line}" for line in code.splitlines())
        return f'=== "{label}"\n\n    ```{lang}\n{indented}\n    ```\n'

    body = (
        tab("SQL", "sql", sql)
        + "\n"
        + tab("YAML", "yaml", yaml)
        + "\n"
        + tab("Python", "python", python_decl)
        + "\n"
        + tab("Rivet API", "python", python_api)
    )

    if title:
        return f"{prefix}\n{body}\n{suffix}"
    return body
