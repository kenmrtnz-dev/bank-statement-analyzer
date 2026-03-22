from app import pdf_text_extract


def test_extract_pdf_layout_pages_ignores_pdftotext_warnings(monkeypatch):
    noisy_xml = """Syntax Error (1124032): Bad block header in flate stream
Syntax Error (1994601): Unknown operator
<!DOCTYPE html>
<html>
  <body>
    <doc>
      <page width="612" height="792">
        <word xMin="10" yMin="20" xMax="40" yMax="30">Date</word>
        <word xMin="50" yMin="20" xMax="90" yMax="30">Amount</word>
      </page>
    </doc>
  </body>
</html>
"""
    monkeypatch.setattr(pdf_text_extract.subprocess, "check_output", lambda *args, **kwargs: noisy_xml)

    pages = pdf_text_extract.extract_pdf_layout_pages("dummy.pdf")

    assert len(pages) == 1
    assert pages[0]["text"] == "Date Amount"
    assert pages[0]["words"][0]["text"] == "Date"


def test_extract_pdf_layout_pages_ignores_warnings_injected_mid_tag(monkeypatch):
    noisy_xml = """<!DOCTYPE html>
<html>
  <body>
    <doc>
      <page width="612" height="792">
        <line xMin="10" yMin="20" xMax="90" yMaSyntax Error (99): Bad block header in flate stream
Syntax Error (100): Bad block header in flate stream
x="30">
          <word xMin="10" yMin="20" xMax="40" yMax="30">Date</word>
        </line>
      </page>
    </doc>
  </body>
</html>
"""
    monkeypatch.setattr(pdf_text_extract.subprocess, "check_output", lambda *args, **kwargs: noisy_xml)

    pages = pdf_text_extract.extract_pdf_layout_pages("dummy.pdf")

    assert len(pages) == 1
    assert pages[0]["text"] == "Date"
