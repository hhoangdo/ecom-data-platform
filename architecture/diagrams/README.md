# Diagram Editing Notes

## Excalidraw

- Primary file: `architecture.excalidraw`
- If VS Code cannot open the custom editor, use **Reopen Editor With...** and choose **Text Editor** to inspect/fix raw JSON.
- Keep elements in canonical Excalidraw JSON format (with required element metadata fields), not simplified `label`-only shapes.

## PlantUML

- Primary file: `lambda_architecture.puml`
- The current consumption-layer labels name Apache Pinot and DuckDB as Section `02` implementation targets. Section `01` still owns source contracts and architecture documentation only.
- Workspace defaults in `.vscode/settings.json` use PlantUML server rendering:
  - `"plantuml.render": "PlantUMLServer"`
  - `"plantuml.server": "https://www.plantuml.com/plantuml"`
- Use PlantUML for system and architecture diagrams such as `lambda_architecture.puml`, `detailed-lambda_architecture.puml`, `schema_design.puml`, and `physical_gold_model.puml`.

## DBML

- Primary file for the readable Gold-only ERD: `gold_layer_ERD.dbml`
- Use the official dbdiagram VS Code extension preview button, or run **DBML: Open Preview to the Side** from the Command Palette.
- `gold_layer_ERD.dbml` is the readable Gold-layer ERD for day-to-day inspection.
- `physical_gold_model.puml` remains the exhaustive physical/logical reference across Bronze, Silver, and Gold.
- `.vscode/extensions.json` recommends the diagram and docs extensions this repo expects contributors to have available.

## Optional Offline PlantUML Setup

If you want local/offline rendering instead of the remote server:

1. Install Java and Graphviz (`dot`).
2. Verify:
   - `java -version`
   - `dot -V`
3. Change setting to `"plantuml.render": "Local"` and reload VS Code.
