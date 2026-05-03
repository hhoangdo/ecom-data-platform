from pathlib import Path

import vina_bim_shop


def test_project_scaffold_artifacts_exist() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    assert vina_bim_shop.__doc__ == "Vina Bim Shop coursework package."
    assert (repo_root / "architecture" / "masterplan.md").is_file()
    assert (repo_root / "configs" / "generator" / "base.yaml").is_file()
    assert (
        repo_root / "data" / "reference" / "taxonomy" / "taxonomy_snapshot.yaml"
    ).is_file()
