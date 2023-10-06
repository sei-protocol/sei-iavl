package changeset

import (
	"errors"
	"io"
)

type WrapReader struct {
	readerCloser io.ReadCloser
	byteReader   io.ByteReader
	closer       io.Closer
}

func (r *WrapReader) Read(p []byte) (int, error) {
	return r.readerCloser.Read(p)
}

func (r *WrapReader) ReadByte() (byte, error) {
	return r.byteReader.ReadByte()
}

func (r *WrapReader) Close() error {
	var errs []error
	if closer, ok := r.readerCloser.(io.Closer); ok {
		errs = append(errs, closer.Close())
	}
	if r.closer != nil {
		errs = append(errs, r.closer.Close())
	}
	return errors.Join(errs...)
}
