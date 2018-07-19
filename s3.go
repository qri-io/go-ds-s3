package s3

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	datastore "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

// Datastore is an implementation of the IPFS Datastore interface for Amazon S3 (Simple Storage Service)
type Datastore struct {
	Path         string
	Bucket       string
	Region       string
	accessKey    string
	accessSecret string
	accessToken  string
	s3           *awsS3.S3
}

// assert *Datastore satisfies datastore.Datastore interface at compile time
var _ datastore.Datastore = (*Datastore)(nil)

// NewDatastore creates a new datastore, accepting zero or more functions that modify options
func NewDatastore(bucketName string, options ...func(o *Options)) *Datastore {
	opts := DefaultOptions()
	// apply options
	for _, fn := range options {
		fn(opts)
	}

	return &Datastore{
		Path:         opts.Path,
		Bucket:       bucketName,
		Region:       opts.Region,
		accessKey:    opts.AccessKey,
		accessSecret: opts.AccessSecret,
		accessToken:  opts.AccessToken,
	}
}

// Options configures a Datastore. DefaultOptions sets default values
// which can be modified by passing func(s) to NewDatastore
type Options struct {
	// Scope to a specific "folder" within the bucket without leading or trailing slashes. eg "folder" or "folder/subfolder"
	Path string
	// The AWS region this bucket is located in. Default regin since March 8, 2013 is "us-west-2"
	// see: http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region for regions list
	Region string
	// a valid access key for the named bucket is required, defaults to AWS_ACCESS_KEY_ID ENV variable
	AccessKey string
	// a valid access key for the named bucket is required, defaults to AWS_SECRET_ACCESS_KEY ENV variable
	AccessSecret string
	// AccessToken is only required when using temporary credentials, defaults to AWS_SESSION_TOKEN ENV variable
	AccessToken string
}

// DefaultOptions is the base set of options provided to New()
func DefaultOptions() *Options {
	return &Options{
		Region:       "us-west-2",
		AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		AccessSecret: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AccessToken:  os.Getenv("AWS_SESSION_TOKEN"),
	}
}

// Put an object into the store
func (ds *Datastore) Put(key datastore.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return datastore.ErrInvalidType
	}

	c := ds.client()
	_, err := c.PutObject(&awsS3.PutObjectInput{
		Bucket: aws.String(ds.Bucket),
		Key:    aws.String(ds.path(key)),
		Body:   bytes.NewReader(val),
	})

	return err
}

func (_ *Datastore) Close() error {
	return nil
}

// Get an object from the store
func (ds *Datastore) Get(key datastore.Key) (value interface{}, err error) {
	c := ds.client()
	res, err := c.GetObject(&awsS3.GetObjectInput{
		Key:    aws.String(ds.path(key)),
		Bucket: aws.String(ds.Bucket),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NoSuchKey" {
				return nil, datastore.ErrNotFound
			}
		}
		return nil, err
	}
	defer res.Body.Close()

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, res.Body)

	return buf.Bytes(), err
}

// Has checks for the presence of a key within the store
func (ds *Datastore) Has(key datastore.Key) (exists bool, err error) {
	c := ds.client()
	_, err = c.HeadObject(&awsS3.HeadObjectInput{
		Bucket: aws.String(ds.Bucket),
		Key:    aws.String(ds.path(key)),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

// Delete a key from the store
func (ds *Datastore) Delete(key datastore.Key) error {
	c := ds.client()

	if has, err := ds.Has(key); has == false {
		return datastore.ErrNotFound
	} else if err != nil {
		return err
	}

	_, err := c.DeleteObject(&awsS3.DeleteObjectInput{
		Key:    aws.String(ds.path(key)),
		Bucket: aws.String(ds.Bucket),
	})

	return err
}

// Query the store
func (ds *Datastore) Query(q query.Query) (query.Results, error) {
	// TODO - support query Filters
	if len(q.Filters) > 0 {
		return nil, errors.New("s3 datastore queries do not support filters")
	}
	// TODO - support query Orders
	if len(q.Orders) > 0 {
		return nil, errors.New("s3 datastore queries do note support ordering")
	}

	c := ds.client()
	res, err := c.ListObjects(&awsS3.ListObjectsInput{
		Bucket: aws.String(ds.Bucket),
		Prefix: aws.String(ds.stringPath(q.Prefix)),
	})
	if err != nil {
		return nil, err
	}

	if q.KeysOnly {
		entries := []query.Entry{}
		added := 0
		for i, obj := range res.Contents {
			if q.Offset > 0 && i <= q.Offset {
				continue
			}
			if q.Limit > 0 && added == q.Limit {
				break
			}

			entries = append(entries, query.Entry{Key: ds.key(aws.StringValue(obj.Key)).String()})
			added++
		}
		return query.ResultsWithEntries(q, entries), nil
	}

	reschan := make(chan query.Result, query.NormalBufSize)
	go func() {
		defer close(reschan)

		added := 0
		for i, obj := range res.Contents {
			key := ds.key(aws.StringValue(obj.Key))
			if q.Offset > 0 && i <= q.Offset {
				continue
			}
			if q.Limit > 0 && added == q.Limit {
				break
			}

			value, err := ds.Get(key)
			if err != nil {
				reschan <- query.Result{Error: err}
				break
			}

			reschan <- query.Result{
				Entry: query.Entry{
					Key:   key.String(),
					Value: value,
				},
			}
			added++
		}
	}()

	return query.ResultsWithChan(q, reschan), nil
}

type Batch struct {
	s *Datastore
}

func (ds *Datastore) Batch() (datastore.Batch, error) {
	return &Batch{ds}, nil
}

func (b *Batch) Put(k datastore.Key, val interface{}) error {
	return b.s.Put(k, val)
}

func (b *Batch) Delete(k datastore.Key) error {
	return b.s.Delete(k)
}

func (b *Batch) Commit() error {
	return nil
}

var _ datastore.Batching = (*Datastore)(nil)

// svc gives an aws.S3 client instance
func (ds *Datastore) client() *awsS3.S3 {
	if ds.s3 != nil {
		return ds.s3
	}

	ds.s3 = awsS3.New(session.New(&aws.Config{
		Region:      aws.String(ds.Region),
		Credentials: credentials.NewStaticCredentials(ds.accessKey, ds.accessSecret, ds.accessToken),
	}))
	return ds.s3
}

// path creates the full path to an object by appending the bucket path to key.Path
func (ds *Datastore) path(key datastore.Key) string {
	return strings.TrimLeft(ds.Path+key.String(), "/")
	// return strings.TrimLeft(filepath.Join(ds.Path, key.String()), "/")
}

// path creates the full path to an object by appending the bucket path to key.Path
func (ds *Datastore) stringPath(path string) string {
	return strings.TrimLeft(ds.Path+path, "/")
}

// key returns a key from a full object path, removing the ds.Path prefix
func (ds *Datastore) key(fullPath string) datastore.Key {
	return datastore.NewKey(strings.TrimPrefix(fullPath, ds.Path))
}
