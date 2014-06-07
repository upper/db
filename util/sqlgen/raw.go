package sqlgen

type Raw struct {
	Raw string
}

func (self Raw) String() string {
	return self.Raw
}
