# Diagram Editing Notes

## Excalidraw

- Primary file: `architecture.excalidraw`
- If VS Code cannot open the custom editor, use **Reopen Editor With...** and choose **Text Editor** to inspect/fix raw JSON.
- Keep elements in canonical Excalidraw JSON format (with required element metadata fields), not simplified `label`-only shapes.

## PlantUML

- Primary file: `lambda_architecture.puml`
- Workspace defaults in `.vscode/settings.json` use PlantUML server rendering:
  - `"plantuml.render": "PlantUMLServer"`
  - `"plantuml.server": "https://www.plantuml.com/plantuml"`

## Optional Offline PlantUML Setup

If you want local/offline rendering instead of the remote server:

1. Install Java and Graphviz (`dot`).
2. Verify:
   - `java -version`
   - `dot -V`
3. Change setting to `"plantuml.render": "Local"` and reload VS Code.
