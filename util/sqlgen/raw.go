package sqlgen

type Raw struct {
	Raw string
}

func (self Raw) Hash() string {
	return `Raw(` + self.Raw + `)`
}

func (self Raw) Compile(*Template) string {
	return self.Raw
}

func (self Raw) String() string {
	return self.Raw
}
