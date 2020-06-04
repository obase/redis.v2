package redis

type BulkResp struct {
	Reply interface{}
	Error error
}

func (b *BulkResp) Bool() (bool, bool, error) {
	return Bool(b.Reply, b.Error)
}

func (b *BulkResp) Int() (int, bool, error) {
	return Int(b.Reply, b.Error)
}

func (b *BulkResp) Int64() (int64, bool, error) {
	return Int64(b.Reply, b.Error)
}

func (b *BulkResp) Uint64() (uint64, bool, error) {
	return Uint64(b.Reply, b.Error)
}

func (b *BulkResp) Float64() (float64, bool, error) {
	return Float64(b.Reply, b.Error)
}

func (b *BulkResp) String() (string, bool, error) {
	return String(b.Reply, b.Error)
}

func (b *BulkResp) Bytes() ([]byte, bool, error) {
	return Bytes(b.Reply, b.Error)
}
func (b *BulkResp) Slice() ([]interface{}, bool, error) {
	return Slice(b.Reply, b.Error)
}
func (b *BulkResp) ValueScoreSlice() ([]*ValueScore, bool, error) {
	return ValueScoreSlice(b.Reply, b.Error)
}

func (b *BulkResp) BoolSlice() ([]bool, bool, error) {
	return BoolSlice(b.Reply, b.Error)
}

func (b *BulkResp) IntSlice() ([]int, bool, error) {
	return IntSlice(b.Reply, b.Error)
}

func (b *BulkResp) Int64Slice() ([]int64, bool, error) {
	return Int64Slice(b.Reply, b.Error)
}

func (b *BulkResp) Uint64Slice() ([]uint64, bool, error) {
	return Uint64Slice(b.Reply, b.Error)
}

func (b *BulkResp) Float64Slice() ([]float64, bool, error) {
	return Float64Slice(b.Reply, b.Error)
}

func (b *BulkResp) StringSlice() ([]string, bool, error) {
	return StringSlice(b.Reply, b.Error)
}

func (b *BulkResp) BytesSlice() ([][]byte, bool, error) {
	return BytesSlice(b.Reply, b.Error)
}

func (b *BulkResp) Map() (map[string]interface{}, bool, error) {
	return Map(b.Reply, b.Error)
}

func (b *BulkResp) BoolMap() (map[string]bool, bool, error) {
	return BoolMap(b.Reply, b.Error)
}

func (b *BulkResp) IntMap() (map[string]int, bool, error) {
	return IntMap(b.Reply, b.Error)
}

func (b *BulkResp) Int64Map() (map[string]int64, bool, error) {
	return Int64Map(b.Reply, b.Error)
}

func (b *BulkResp) Uint64Map() (map[string]uint64, bool, error) {
	return Uint64Map(b.Reply, b.Error)
}

func (b *BulkResp) Float64Map() (map[string]float64, bool, error) {
	return Float64Map(b.Reply, b.Error)
}

func (b *BulkResp) StringMap() (map[string]string, bool, error) {
	return StringMap(b.Reply, b.Error)
}

func (b *BulkResp) BytesMap() (map[string][]byte, bool, error) {
	return BytesMap(b.Reply, b.Error)
}
