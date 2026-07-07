package dat

import "fmt"

// Structure holds the schema-free layout metrics of a DAT file, derived purely
// from the row-count header and the 0xBB boundary marker separating the fixed
// row section from the variable-length data.
type Structure struct {
	RowCount  int
	RowWidth  int
	FixedSize int
	VarOffset int
	VarSize   int
}

// ParseStructure derives a DAT file's layout without a schema. RowWidth is the
// fixed section length divided by the row count (zero when there are no rows),
// and VarOffset points at the boundary marker where the variable section begins.
func ParseStructure(data []byte) (Structure, error) {
	if len(data) < MinDATFileSize {
		return Structure{}, fmt.Errorf("DAT file too small: %d bytes (minimum %d)", len(data), MinDATFileSize)
	}

	df, err := parseDATStructure(data)
	if err != nil {
		return Structure{}, err
	}

	fixedSize := len(df.FixedData)
	rowWidth := 0
	if df.RowCount > 0 {
		rowWidth = fixedSize / df.RowCount
	}

	return Structure{
		RowCount:  df.RowCount,
		RowWidth:  rowWidth,
		FixedSize: fixedSize,
		VarOffset: 4 + fixedSize,
		VarSize:   len(df.DynamicData),
	}, nil
}
