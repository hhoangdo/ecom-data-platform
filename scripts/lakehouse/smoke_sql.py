from __future__ import annotations

from vina_bim_shop.lakehouse.smoke import run_smoke_sql


def main() -> None:
    print(run_smoke_sql())


if __name__ == "__main__":
    main()
