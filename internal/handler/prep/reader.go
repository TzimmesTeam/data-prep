package prep

import (
	"archive/zip"
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/TzimmesTeam/data-prep/internal/domain/models"
)

var errFieldCount = errors.New("wrong number of fields")

type rowReader interface {
	Read() ([]string, int, error)
	SetWidth(n int)
	Close() error
	ExcelDates() (enabled, date1904 bool)
}

func newRowReader(r io.Reader, format models.SourceFormat) (rowReader, error) {
	switch format {
	case models.FormatCSV:
		return newCSVReader(r), nil
	case models.FormatXLSX:
		return newXLSXReader(r)
	default:
		return nil, fmt.Errorf("unsupported source format: %s", format)
	}
}

type csvReader struct {
	reader *csv.Reader
	row    int
}

func newCSVReader(r io.Reader) *csvReader {
	reader := csv.NewReader(r)
	reader.Comma = ';'
	reader.TrimLeadingSpace = true
	return &csvReader{reader: reader}
}

func (c *csvReader) Read() ([]string, int, error) {
	row, err := c.reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, c.row, io.EOF
	}
	c.row++
	if errors.Is(err, csv.ErrFieldCount) {
		return row, c.row, fmt.Errorf("%w: %w", errFieldCount, err)
	}
	return row, c.row, err
}

func (c *csvReader) SetWidth(n int) { c.reader.FieldsPerRecord = n }

func (c *csvReader) Close() error { return nil }

func (c *csvReader) ExcelDates() (bool, bool) { return false, false }

type xlsxReader struct {
	file     *os.File
	sheet    io.ReadCloser
	decoder  *xml.Decoder
	strings  []string
	date1904 bool
	width    int
	row      int
	done     bool
}

func newXLSXReader(r io.Reader) (*xlsxReader, error) {
	file, err := os.CreateTemp("", "data-prep-*.xlsx")
	if err != nil {
		return nil, fmt.Errorf("create workbook temp file: %w", err)
	}
	reader := &xlsxReader{file: file}
	if err := reader.open(r); err != nil {
		return nil, errors.Join(err, reader.Close())
	}
	return reader, nil
}

func (x *xlsxReader) open(r io.Reader) error {
	size, err := io.Copy(x.file, r)
	if err != nil {
		return fmt.Errorf("spool workbook: %w", err)
	}
	archive, err := zip.NewReader(x.file, size)
	if err != nil {
		return fmt.Errorf("open workbook: %w", err)
	}
	entries := make(map[string]*zip.File, len(archive.File))
	for _, entry := range archive.File {
		entries[entry.Name] = entry
	}

	workbookPath := officeDocumentPath(entries)
	sheetPath, sharedStringsPath, err := x.readWorkbook(entries, workbookPath)
	if err != nil {
		return err
	}
	if entry, ok := entries[sharedStringsPath]; ok {
		if x.strings, err = readSharedStrings(entry); err != nil {
			return err
		}
	}
	entry, ok := entries[sheetPath]
	if !ok {
		return fmt.Errorf("worksheet %q not found in workbook", sheetPath)
	}
	if x.sheet, err = entry.Open(); err != nil {
		return fmt.Errorf("open worksheet %q: %w", sheetPath, err)
	}
	x.decoder = xml.NewDecoder(x.sheet)
	return nil
}

type xlsxRelationship struct {
	ID     string `xml:"Id,attr"`
	Type   string `xml:"Type,attr"`
	Target string `xml:"Target,attr"`
}

type xlsxRelationships struct {
	Items []xlsxRelationship `xml:"Relationship"`
}

type xlsxWorkbook struct {
	Props struct {
		Date1904 string `xml:"date1904,attr"`
	} `xml:"workbookPr"`
	Sheets []struct {
		Attrs []xml.Attr `xml:",any,attr"`
	} `xml:"sheets>sheet"`
}

func decodeEntry(entry *zip.File, target any) error {
	content, err := entry.Open()
	if err != nil {
		return fmt.Errorf("open %q: %w", entry.Name, err)
	}
	defer func() { _ = content.Close() }()
	if err := xml.NewDecoder(content).Decode(target); err != nil {
		return fmt.Errorf("decode %q: %w", entry.Name, err)
	}
	return nil
}

func officeDocumentPath(entries map[string]*zip.File) string {
	entry, ok := entries["_rels/.rels"]
	if !ok {
		return "xl/workbook.xml"
	}
	var rels xlsxRelationships
	if err := decodeEntry(entry, &rels); err != nil {
		return "xl/workbook.xml"
	}
	for _, rel := range rels.Items {
		if strings.HasSuffix(rel.Type, "/officeDocument") {
			return resolvePartPath("", rel.Target)
		}
	}
	return "xl/workbook.xml"
}

func (x *xlsxReader) readWorkbook(entries map[string]*zip.File, workbookPath string) (string, string, error) {
	entry, ok := entries[workbookPath]
	if !ok {
		return "", "", fmt.Errorf("workbook part %q not found", workbookPath)
	}
	var workbook xlsxWorkbook
	if err := decodeEntry(entry, &workbook); err != nil {
		return "", "", err
	}
	x.date1904 = workbook.Props.Date1904 == "1" || strings.EqualFold(workbook.Props.Date1904, "true")
	if len(workbook.Sheets) == 0 {
		return "", "", fmt.Errorf("workbook has no worksheets")
	}
	sheetID := ""
	for _, attr := range workbook.Sheets[0].Attrs {
		if attr.Name.Local == "id" && attr.Name.Space != "" {
			sheetID = attr.Value
		}
	}

	baseDir := path.Dir(workbookPath)
	relsPath := path.Join(baseDir, "_rels", path.Base(workbookPath)+".rels")
	relsEntry, ok := entries[relsPath]
	if !ok {
		return "", "", fmt.Errorf("workbook relationships %q not found", relsPath)
	}
	var rels xlsxRelationships
	if err := decodeEntry(relsEntry, &rels); err != nil {
		return "", "", err
	}
	sheetPath := ""
	sharedStringsPath := path.Join(baseDir, "sharedStrings.xml")
	for _, rel := range rels.Items {
		if rel.ID == sheetID && sheetID != "" {
			sheetPath = resolvePartPath(baseDir, rel.Target)
		}
		if strings.HasSuffix(rel.Type, "/sharedStrings") {
			sharedStringsPath = resolvePartPath(baseDir, rel.Target)
		}
	}
	if sheetPath == "" {
		return "", "", fmt.Errorf("first worksheet relationship %q not found", sheetID)
	}
	return sheetPath, sharedStringsPath, nil
}

func resolvePartPath(baseDir, target string) string {
	if strings.HasPrefix(target, "/") {
		return strings.TrimPrefix(target, "/")
	}
	return path.Join(baseDir, target)
}

type sharedStringsState struct {
	result   []string
	text     strings.Builder
	inItem   bool
	inText   bool
	phonetic int
}

func (st *sharedStringsState) start(name string) {
	switch name {
	case "si":
		st.inItem = true
		st.text.Reset()
	case "rPh":
		st.phonetic++
	case "t":
		st.inText = st.inItem && st.phonetic == 0
	}
}

func (st *sharedStringsState) end(name string) {
	switch name {
	case "si":
		st.inItem = false
		st.result = append(st.result, st.text.String())
	case "rPh":
		st.phonetic--
	case "t":
		st.inText = false
	}
}

func readSharedStrings(entry *zip.File) ([]string, error) {
	content, err := entry.Open()
	if err != nil {
		return nil, fmt.Errorf("open shared strings: %w", err)
	}
	defer func() { _ = content.Close() }()
	decoder := xml.NewDecoder(content)
	state := &sharedStringsState{}
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return state.result, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode shared strings: %w", err)
		}
		switch t := token.(type) {
		case xml.StartElement:
			state.start(t.Name.Local)
		case xml.EndElement:
			state.end(t.Name.Local)
		case xml.CharData:
			if state.inText {
				state.text.Write(t)
			}
		}
	}
}

func (x *xlsxReader) Read() ([]string, int, error) {
	for {
		row, err := x.nextRow()
		if err != nil {
			return nil, x.row, err
		}
		if blankRow(row) {
			continue
		}
		if x.width == 0 {
			return row, x.row, nil
		}
		return x.fit(row)
	}
}

type xlsxCell struct {
	column int
	kind   string
	value  strings.Builder
}

type rowState struct {
	row      []string
	cell     *xlsxCell
	inRow    bool
	inValue  bool
	inInline bool
	phonetic int
	complete bool
	end      bool
}

func (x *xlsxReader) nextRow() ([]string, error) {
	if x.done {
		return nil, io.EOF
	}
	state := &rowState{}
	for {
		token, err := x.decoder.Token()
		if errors.Is(err, io.EOF) {
			x.done = true
			if state.inRow {
				return nil, fmt.Errorf("worksheet ended inside row %d", x.row)
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("decode worksheet: %w", err)
		}
		if err := x.handleToken(state, token); err != nil {
			return nil, err
		}
		if state.end {
			x.done = true
			return nil, io.EOF
		}
		if state.complete {
			return state.row, nil
		}
	}
}

func (x *xlsxReader) handleToken(state *rowState, token xml.Token) error {
	switch t := token.(type) {
	case xml.StartElement:
		return x.handleStart(state, t)
	case xml.EndElement:
		return x.handleEnd(state, t.Name.Local)
	case xml.CharData:
		if state.inValue && state.cell != nil {
			state.cell.value.Write(t)
		}
	}
	return nil
}

func (x *xlsxReader) handleStart(state *rowState, element xml.StartElement) error {
	switch element.Name.Local {
	case "row":
		return x.startRow(state, element)
	case "c":
		return startCell(state, element)
	case "v":
		state.inValue = state.cell != nil
	case "is":
		state.inInline = state.cell != nil
	case "rPh":
		state.phonetic++
	case "t":
		state.inValue = state.inInline && state.phonetic == 0
	}
	return nil
}

func (x *xlsxReader) startRow(state *rowState, element xml.StartElement) error {
	state.inRow = true
	x.row++
	number, ok := attr(element, "r")
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(number)
	if err != nil || parsed < 1 {
		return fmt.Errorf("invalid row reference %q", number)
	}
	x.row = parsed
	return nil
}

func startCell(state *rowState, element xml.StartElement) error {
	if !state.inRow {
		return nil
	}
	state.cell = &xlsxCell{column: len(state.row)}
	state.cell.kind, _ = attr(element, "t")
	ref, ok := attr(element, "r")
	if !ok {
		return nil
	}
	column, err := columnIndex(ref)
	if err != nil {
		return err
	}
	state.cell.column = column
	return nil
}

func (x *xlsxReader) handleEnd(state *rowState, name string) error {
	switch name {
	case "row":
		state.complete = true
	case "c":
		return x.endCell(state)
	case "v", "t":
		state.inValue = false
	case "is":
		state.inInline = false
	case "rPh":
		state.phonetic--
	case "sheetData":
		state.end = true
	}
	return nil
}

func (x *xlsxReader) endCell(state *rowState) error {
	cell := state.cell
	state.cell, state.inValue, state.inInline = nil, false, false
	if cell == nil {
		return nil
	}
	value, err := x.cellValue(cell)
	if err != nil {
		return err
	}
	for len(state.row) <= cell.column {
		state.row = append(state.row, "")
	}
	state.row[cell.column] = value
	return nil
}

func (x *xlsxReader) cellValue(cell *xlsxCell) (string, error) {
	value := cell.value.String()
	switch cell.kind {
	case "s":
		index, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || index < 0 || index >= len(x.strings) {
			return "", fmt.Errorf("row %d: invalid shared string index %q", x.row, value)
		}
		return x.strings[index], nil
	case "d":
		date, _, _ := strings.Cut(value, "T")
		return date, nil
	default:
		return value, nil
	}
}

func attr(element xml.StartElement, name string) (string, bool) {
	for _, a := range element.Attr {
		if a.Name.Local == name && a.Name.Space == "" {
			return a.Value, true
		}
	}
	return "", false
}

func columnIndex(ref string) (int, error) {
	column := 0
	letters := 0
	for _, r := range strings.ToUpper(ref) {
		if r < 'A' || r > 'Z' {
			break
		}
		column = column*26 + int(r-'A'+1)
		letters++
	}
	if letters == 0 || letters > 3 {
		return 0, fmt.Errorf("invalid cell reference %q", ref)
	}
	return column - 1, nil
}

func (x *xlsxReader) fit(row []string) ([]string, int, error) {
	if len(row) > x.width {
		if !blankRow(row[x.width:]) {
			return row, x.row, fmt.Errorf("%w: row %d has %d fields, header has %d", errFieldCount, x.row, len(row), x.width)
		}
		row = row[:x.width]
	}
	for len(row) < x.width {
		row = append(row, "")
	}
	return row, x.row, nil
}

func (x *xlsxReader) SetWidth(n int) { x.width = n }

func (x *xlsxReader) Close() error {
	var errs []error
	if x.sheet != nil {
		errs = append(errs, x.sheet.Close())
	}
	if x.file != nil {
		errs = append(errs, x.file.Close(), os.Remove(x.file.Name()))
	}
	return errors.Join(errs...)
}

func (x *xlsxReader) ExcelDates() (bool, bool) { return true, x.date1904 }

func blankRow(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}
