"""Run one pipeline stage or a diagnostic without loading unrelated services."""

import argparse
import logging
import os
from importlib import import_module

from job_market.config import SOURCES, load_settings

COMMANDS = {
    "crawl": ("ingestion.{source}.crawl", "Collect job listings"),
    "enrich": ("ingestion.{source}.enrich", "Fetch full job descriptions"),
    "ingest": ("warehouse.ingest", "Load enriched Parquet into Bronze"),
    "extract": ("enrichment.extract", "Extract structured fields from job descriptions"),
    "sync-search": ("search.sync", "Update the Qdrant search index"),
    "verify-expired": ("operations.verify_expired", "Verify older listings and mark expired jobs in Bronze"),
    "inspect": ("operations.inspect", "Inspect warehouse tables or the search index"),
}


def main(argv: list[str] | None = None) -> int:
    """Parse the CLI arguments and run the selected stage."""
    parser = argparse.ArgumentParser(description="IT job market pipeline")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command, (_, description) in COMMANDS.items():
        stage = subcommands.add_parser(command, help=description, description=description)
        if command in {"crawl", "enrich", "extract"}:
            stage.add_argument("source", choices=SOURCES)
        if command == "inspect":
            stage.add_argument("target", choices=("warehouse", "search"))
    args = parser.parse_args(argv)
    settings = load_settings()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    source = getattr(args, "source", None)
    module, _ = COMMANDS[args.command]
    stage = import_module("job_market." + module.format(source=source))
    if args.command == "extract":
        return stage.run(settings, source)
    if args.command == "inspect":
        return stage.run(settings, args.target)
    return stage.run(settings)


if __name__ == "__main__":
    raise SystemExit(main())
