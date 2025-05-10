from html2text import HTML2Text


def transform_description_html(html: str) -> str:
    # Python version of node.js getIdcCollectionMetadata
    # handle oddly-formatted IDC response HTML
    converter = HTML2Text()
    converter.ignore_links = True
    converter.body_width = 0
    converter.ignore_emphasis = True
    converter.single_line_break = True

    text = converter.handle(html)
    text = text.replace("\n", " ").replace("  ", " ").replace("    ", " ")

    return text.strip()
