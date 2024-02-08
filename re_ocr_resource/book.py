from lxml import etree

# from tqdm.notebook import tqdm
import os
from typing import NamedTuple, Iterable, List
from dataclasses import dataclass
import pathlib
from tqdm.contrib.concurrent import process_map
import glob
from functools import partial

## Logging
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing

URN_PREFIX = "URN:NBN:"


@dataclass
class AltoString:
    id: str
    token: str


@dataclass
class TextLine:
    id: str
    strings: Iterable[AltoString]


@dataclass
class TextBlock:
    id: str
    text_lines: str


@dataclass
class ComposedBlock:
    id: str
    text_blocks: Iterable[TextBlock]


@dataclass
class PageData:
    composed_blocks: Iterable[ComposedBlock]


class Book:
    def __init__(self, path: str):
        self.path = path
        self.pages: List[Page] = []
        self.parse()

    @property
    def urn_suffix(self):
        return os.path.basename(self.path).split(".")[0]

    @property
    def urn(self):
        return URN_PREFIX + self.urn_suffix

    def __repr__(self):
        return f"<Book {self.urn}>"

    def parse(self):
        for path, _, files in os.walk(self.path):
            for file in files:
                if file.endswith(".xml"):
                    self.pages.append(Page(os.path.join(path, file)))

    def print_pages(self):
        for page in self.pages:
            print(page.path)

    def __getitem__(self, slice):
        return self.pages[slice]

    def export_to_txt(self, other_path: str):
        os.makedirs(other_path, exist_ok=True)
        for page in self.pages:
            with open(os.path.join(other_path, page.urn_suffix + ".txt"), "w") as f:
                for line in page.text:
                    f.write(line)


class Page:
    def __init__(self, path: str):
        self.path = path
        self.components: List[PageData] = []
        self.parse()

    @property
    def urn_suffix(self):
        return os.path.basename(self.path).split(".")[0]

    @property
    def urn(self):
        return URN_PREFIX + self.urn_suffix

    def __repr__(self):
        return f"<Page {self.urn}>"

    def parse(self):
        # Parse the ALTO XML file
        tree = etree.parse(self.path)
        namespaces = {"alto": "http://www.loc.gov/standards/alto/ns-v3#"}

        data = PageData([])
        self.components.append(data)

        composed_blocks = tree.xpath(".//alto:ComposedBlock", namespaces=namespaces)

        for composed_block in composed_blocks:
            id = composed_block.get("ID")
            target_composed_block = ComposedBlock(id, [])
            data.composed_blocks.append(target_composed_block)

            blocks = composed_block.xpath("./alto:TextBlock", namespaces=namespaces)
            for block in blocks:
                target_block = TextBlock(block.get("ID"), [])
                target_composed_block.text_blocks.append(target_block)

                lines = block.xpath("./alto:TextLine", namespaces=namespaces)

                for line in lines:
                    target_line = TextLine(line.get("ID"), [])
                    target_block.text_lines.append(target_line)

                    strings = line.xpath("./alto:String", namespaces=namespaces)
                    strings = [
                        AltoString(s.get("ID"), s.get("CONTENT")) for s in strings
                    ]
                    target_line.strings = strings

    def print_text(self):
        for data in self.components:
            for composed_block in data.composed_blocks:
                for block in composed_block.text_blocks:
                    for line in block.text_lines:
                        print(" ".join([t.token for t in line.strings]))
                print()
            print()

    @property
    def text(self):
        for data in self.components:
            for composed_block in data.composed_blocks:
                for block in composed_block.text_blocks:
                    for line in block.text_lines:
                        yield " ".join([t.token for t in line.strings])
                    yield "\n"
                yield "\n"
            yield "\n"


def parse_book(source_path: str, target_path: str):
    # count = 1

    try:
        book = Book(source_path)
        target_path = os.path.join(target_path, book.urn_suffix)

        book.export_to_txt(target_path)
    except Exception as e:
        # error_path = pathlib.Path("/home/larsm/projects/re-ocr-resource/errors")
        # error_path.mkdir(exist_ok=True, parents=True)

        # file_name = "error_" + str(count) + ".txt"
        # with open(error_path / file_name, "w") as f:
        
        #     f.write(f"File {str(source_path)} failed with {str(e)}")

        # count += 1
        logger.error(f"File {str(source_path)} failed with {str(e)}")


def setup_logging():
    logger = logging.getLogger("MyLogger")
    logger.setLevel(logging.ERROR)

    # Here, 'error.log' is the file where errors will be logged.
    handler = RotatingFileHandler("error.log", maxBytes=10000, backupCount=5)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger


def main():
    logger = setup_logging()

    DATA = pathlib.Path("/mnt/md1/new_2023")

    source_folder = DATA / "bok"

    done = source_folder / "done"
    failed = source_folder / "failed"

    target_folder = DATA / "bok_txt"

    target_folder.mkdir(exist_ok=True, parents=True)

    books_paths = glob.glob(str(done) + "/*")
    books_paths.extend(glob.glob(str(failed) + "/*"))

    process_map(
        partial(parse_book, target_path=str(target_folder)),
        books_paths,
        chunksize=10,
        max_workers=8,
        total=len(books_paths),
    )


if __name__ == "__main__":
    main()
