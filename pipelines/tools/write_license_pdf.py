import sys
import textwrap

def write_pdf(path, title, body):
    lines = [title, ""] + textwrap.wrap(body.replace("\n", " "), 90)
    def esc(s):
        return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    content_lines = ["BT", "/F1 10 Tf", "50 750 Td", "14 TL"]
    first = True
    for line in lines[:70]:
        if first:
            content_lines.append("({}) Tj".format(esc(line)))
            first = False
        else:
            content_lines.append("T* ({}) Tj".format(esc(line)))
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="replace")
    objs = []
    objs.append(b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n")
    objs.append(b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n")
    objs.append(b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n")
    objs.append(b"4 0 obj<< /Length %d >>stream\n" % len(stream) + stream + b"\nendstream\nendobj\n")
    objs.append(b"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n")
    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objs:
        offsets.append(len(out))
        out.extend(obj)
    xref_pos = len(out)
    out.extend(b"xref\n0 %d\n" % (len(objs) + 1))
    out.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        out.extend(b"%010d 00000 n \n" % off)
    out.extend(b"trailer<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref_pos))
    with open(path, "wb") as f:
        f.write(out)

if __name__ == "__main__":
    path = sys.argv[1]
    title = sys.argv[2]
    body = sys.argv[3]
    write_pdf(path, title, body)
