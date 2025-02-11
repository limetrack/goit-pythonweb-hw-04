import argparse
import asyncio
import logging
from pathlib import Path
import aiofiles

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def read_folder(source: Path, output: Path):
    try:
        tasks = []
        for entry in await asyncio.to_thread(list, source.iterdir()):
            if entry.is_file():
                tasks.append(copy_file(entry, output))
            elif entry.is_dir():
                tasks.append(read_folder(entry, output))
        await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"Error reading folder {source}: {e}")


async def copy_file(file_path: Path, output: Path):
    try:
        extension = file_path.suffix[1:] or "unknown"
        target_dir = output / extension

        if not target_dir.exists():
            await asyncio.to_thread(target_dir.mkdir, parents=True, exist_ok=True)

        target_path = target_dir / file_path.name
        async with aiofiles.open(file_path, mode="rb") as src_file:
            async with aiofiles.open(target_path, mode="wb") as dst_file:
                while True:
                    chunk = await src_file.read(1024 * 1024)
                    if not chunk:
                        break
                    await dst_file.write(chunk)

        logging.info(f"Copied: {file_path} -> {target_path}")
    except Exception as e:
        logging.error(f"Error copying file {file_path}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Asynchronous file sorting by extension."
    )
    parser.add_argument("source", type=str, help="Path to the source folder.")
    parser.add_argument("output", type=str, help="Path to the output folder.")
    args = parser.parse_args()

    source_path = Path(args.source).resolve()
    output_path = Path(args.output).resolve()

    if not source_path.exists() or not source_path.is_dir():
        logging.error("The source folder does not exist or is not a directory.")
        return

    logging.info(f"Starting to sort files from {source_path} to {output_path}.")
    asyncio.run(read_folder(source_path, output_path))
    logging.info("File sorting completed.")


if __name__ == "__main__":
    main()
