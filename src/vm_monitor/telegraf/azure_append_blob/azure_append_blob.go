//go:generate ../../../tools/readme_config_includer/generator
package azure_append_blob

// TODO: Clean-up start-up logic and filename creation. Current process is fairly slow and inefficient.

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

//go:embed sample.conf
var sampleConfig string

type AzureAppendBlob struct {
	StorageAccountName   string          `toml:"storage_account_name"`
	ContainerName        string          `toml:"container_name"`
	OutputPath           string          `toml:"output_path"`
	UseBatchFormat       bool            `toml:"use_batch_format"`
	CompressionAlgorithm string          `toml:"compression_algorithm"`
	CompressionLevel     int             `toml:"compression_level"`
	Log                  telegraf.Logger `toml:"-"`
	AzureEndpoint        string          `toml:"azure_endpoint"`

	encoder                internal.ContentEncoder
	serializer             serializers.Serializer
	cred                   *azidentity.DefaultAzureCredential
	client                 *appendblob.Client
	appendBlobOutputNum    int
	appendBlobRetryCount   int
	appendBlobNameTemplate string
	containerURL           string
	appendBlobURL          string
	baseAppendBlobURL      string
}

const (
	defaultAzureEndpoint       = "blob.core.windows.net"
	appendBlobBaseName         = "vm_perf_metrics"
	appendBlobNameBaseTemplate = ".%d.json"
	appendBlobWriteRetryLimit  = 3
	appendBlobMaxBlocks        = 50000

	urlTemplate = "https://%s.%s/%s"
)

func (*AzureAppendBlob) SampleConfig() string {
	return sampleConfig
}

func (f *AzureAppendBlob) SetSerializer(serializer serializers.Serializer) {
	f.serializer = serializer
}

func (f *AzureAppendBlob) Init() error {
	var err error
	f.appendBlobOutputNum = -1
	f.appendBlobRetryCount = 0
	if f.AzureEndpoint == "" {
		f.AzureEndpoint = defaultAzureEndpoint
	}
	if f.StorageAccountName == "" {
		return fmt.Errorf("storage_account_name is required")
	}
	if f.ContainerName == "" {
		return fmt.Errorf("container_name is required")
	}
	if f.OutputPath == "" {
		return fmt.Errorf("OutputPath is required")
	}

	var options []internal.EncodingOption
	if f.CompressionAlgorithm == "" {
		f.CompressionAlgorithm = "identity"
	}

	// Set appendBlob naming convention based on compression algorithm
	switch f.CompressionAlgorithm {
	case "zstd":
		f.appendBlobNameTemplate = strings.Replace(appendBlobNameBaseTemplate, ".json", ".zst", 1)
	case "gzip":
		f.appendBlobNameTemplate = strings.Replace(appendBlobNameBaseTemplate, ".json", ".gz", 1)
	case "zlib":
		f.appendBlobNameTemplate = strings.Replace(appendBlobNameBaseTemplate, ".json", ".zlib", 1)
	default:
		f.appendBlobNameTemplate = appendBlobNameBaseTemplate
	}

	if f.CompressionLevel >= 0 {
		options = append(options, internal.WithCompressionLevel(f.CompressionLevel))
	}
	f.encoder, err = internal.NewContentEncoder(f.CompressionAlgorithm, options...)

	return err
}

func (f *AzureAppendBlob) Connect() error {
	err := f.createAppendBlobLogFile()
	return err
}

func (f *AzureAppendBlob) Close() error {
	var err error
	return err
}

func (f *AzureAppendBlob) Write(metrics []telegraf.Metric) error {
	var writeErr error

	if f.UseBatchFormat {
		octets, err := f.serializer.SerializeBatch(metrics)
		if err != nil {
			f.Log.Debugf("Could not serialize metric: %v", err)
		}

		octets, err = f.encoder.Encode(octets)
		if err != nil {
			f.Log.Debugf("Could not compress metrics: %v", err)
		}

		err = f.WriteToAppendBlob(NewByteReadSeekCloser(octets))
		if err != nil {
			f.Log.Debugf("Error writing to file: %v", err)
		}
	} else {
		for _, metric := range metrics {
			b, err := f.serializer.Serialize(metric)
			if err != nil {
				f.Log.Debugf("Could not serialize metric: %v", err)
			}

			b, err = f.encoder.Encode(b)
			if err != nil {
				f.Log.Debugf("Could not compress metrics: %v", err)
			}

			err = f.WriteToAppendBlob(NewByteReadSeekCloser(b))
			if err != nil {
				f.Log.Debugf("Error writing to write message: %w", err)
			}
		}
	}

	return writeErr
}

func init() {
	outputs.Add("azure_append_blob", func() telegraf.Output {
		return &AzureAppendBlob{
			CompressionLevel: -1,
		}
	})
}

// Sanitize the file input from the config, it should start with a slash
func (f *AzureAppendBlob) sanitizeAppendBlobFilename() string {
	appendBlobFilename := f.OutputPath
	if f.OutputPath[0] != '/' {
		appendBlobFilename = "/" + f.OutputPath
	}
	return appendBlobFilename
}

func (f *AzureAppendBlob) buildBlobURL(outputNumber ...int) error {
	appendBlobFilename := f.sanitizeAppendBlobFilename()

	var blobNumber int
	if len(outputNumber) > 0 {
		blobNumber = outputNumber[0]
	} else {
		blobNumber = f.appendBlobOutputNum
	}

	// Build our append blob URL, the file path is provided as a config input path + potential blob prefix
	// We use the currentAppendBlobNum to track the current append blob number
	f.containerURL = fmt.Sprintf(urlTemplate, f.StorageAccountName, f.AzureEndpoint, f.ContainerName)
	// https://<storageAccountName>.blob.core.windows.net/<containerName>/<appendBlobFilename>"vm_perf_metrics"
	f.baseAppendBlobURL = fmt.Sprintf("%s%s%s", f.containerURL, appendBlobFilename, appendBlobBaseName)
	// https://<storageAccountName>.blob.core.windows.net/<containerName>/<appendBlobFilename>"vm_perf_metrics.%d.json"
	f.appendBlobURL = fmt.Sprintf("%s%s", f.baseAppendBlobURL, fmt.Sprintf(f.appendBlobNameTemplate, blobNumber))
	return nil
}

func (f *AzureAppendBlob) appendBlobAvailable(blobURL string) (bool, bool, error) {
	var err error
	blobClient, err := appendblob.NewClient(blobURL, f.cred, nil)
	if err != nil {
		return false, false, err
	}
	get, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		f.Log.Debugf("Blob does not exist at: \"%s\"", blobURL)
		return false, true, nil // Blob doesn't exist, we can write to it
	} else {
		f.Log.Debugf("Blob exists at: \"%s\"", blobURL)
		if *get.BlobType != "AppendBlob" {
			f.Log.Debugf("Blob is not an append blob, it is a \"%s\"", *get.BlobType)
			return true, false, nil // Blob exists but is not an append blob
		} else if get.BlobCommittedBlockCount != nil && *get.BlobCommittedBlockCount >= appendBlobMaxBlocks {
			f.Log.Debugf("Blob is fully committed")
			return true, false, nil // Blob exists but is fully committed
		} else {
			return true, true, nil // Blob exists, is an append blob, and is not fully committed
		}
	}
}

func (f *AzureAppendBlob) createAppendBlobClient(createAppendBlobFile bool) error {
	if createAppendBlobFile {
		// Check to see if we can write to this append blob
		blobExists, blobUsable, err := f.appendBlobAvailable(f.appendBlobURL)
		if err != nil {
			return err
		}
		if blobUsable {
			f.appendBlobOutputNum = 0
			if blobExists {
				f.Log.Debugf("Appending to existing append blob: \"%s\"", f.appendBlobURL)
				return f.createAppendBlobClientHelper(false)
			} else {
				f.Log.Debugf("Creating new append blob: \"%s\"", f.appendBlobURL)
				return f.createAppendBlobClientHelper(true)
			}
		} else {
			return fmt.Errorf("unable to create append blob: \"%s\"", f.appendBlobURL)
		}
	}
	return f.createAppendBlobClientHelper(false)
}

func (f *AzureAppendBlob) createAppendBlobClientHelper(createFile bool) error {
	var err error
	f.client, err = appendblob.NewClient(f.appendBlobURL, f.cred, nil)
	if err != nil {
		return err
	}
	if createFile {
		_, err = f.client.Create(context.Background(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// Called on initialization, figure out if there are already append blobs in the output path, and if so find the latest one
// That is, if we're writing JSON to "vm_perf_metrics.0.json" there are already "vm_perf_metrics.0.json" and "vm_perf_metrics.1.json"
// check to see if we can write to "vm_perf_metrics.1.json" and if not, create "vm_perf_metrics.2.json"
func (f *AzureAppendBlob) findLatestAppendBlob() error {
	var err error

	// Start with the current 0th append blob:
	err = f.buildBlobURL(0)
	if err != nil {
		return err
	}

	// If the 0th append blob already exists make sure we can write to it:
	f.Log.Debugf("Checking for existing 0th append blob: \"%s\"", f.appendBlobURL)
	blobExists, blobUsable, err := f.appendBlobAvailable(f.appendBlobURL)
	if err == nil && blobUsable {
		f.appendBlobOutputNum = 0
		if blobExists {
			f.Log.Debugf("0th Block exists, checking for other blocks: \"%s\"", f.appendBlobURL)
		} else {
			f.Log.Debugf("0th Block does not exist, creating: \"%s\"", f.appendBlobURL)
			return f.createAppendBlobClient(true)
		}
	}

	// More complex case if the 0th blob exists but is not write-able. Rather than incrementing our appendBlobOutputNum and trying again,
	// we will list the blobs that are available and find the highest number, then write to that blob (or a new blob)
	// Start by listing the files in the destination directory:
	containerClient, err := container.NewClient(f.containerURL, f.cred, nil)
	if err != nil {
		return err
	}
	searchPrefix := strings.Replace(f.baseAppendBlobURL, f.containerURL+"/", "", 1)
	f.Log.Debugf("Searching for blobs with prefix: \"%s*\"", searchPrefix)
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &searchPrefix})

	// Find the blob with the largest number in its name.
	// If there's vm_perf_metrics.0.json and vm_perf_metrics.10.json we'll write to vm_perf_metrics.10.json
	maxNum := -1
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return err
		}
		for _, blob := range resp.Segment.BlobItems {
			name := *blob.Name
			// f.Log.Debug("Found blob: ", name)
			extension := regexp.MustCompile(`\.\w+$`).FindString(f.appendBlobNameTemplate) // Extract the extension from the template
			re := regexp.MustCompile(fmt.Sprintf(`^%s\.(\d+)%s.*`, regexp.QuoteMeta(searchPrefix), regexp.QuoteMeta(extension)))
			matches := re.FindStringSubmatch(name)
			if len(matches) > 1 {
				numStr := matches[1] // get the number part
				num, _ := strconv.Atoi(numStr)
				f.Log.Debugf("Found %d with filename: \"%s\"", num, name)
				if num > maxNum {
					maxNum = num
				}
			}
		}
	}
	f.Log.Debugf("Maximum number found: %d", maxNum)

	// If the maxNum is still -1, it means no blob was found, so create one
	if maxNum == -1 {
		f.appendBlobOutputNum = 0
		f.buildBlobURL()
		f.Log.Debugf("No matching blob found, creating a new one: \"%s\"", f.appendBlobURL)
		return f.createAppendBlobClient(true)
	} else {
		// If a blob was found, start writing to the blob with the largest number in its name
		// Note that if the last blob is not write-able we'll catch that during our first write and move to the next blob
		f.appendBlobOutputNum = maxNum
		f.buildBlobURL()
		f.Log.Debugf("Found existing blob, appending to [%d]: \"%s\"", f.appendBlobOutputNum, f.appendBlobURL)
		return f.createAppendBlobClient(false)
	}
}
func (f *AzureAppendBlob) createAppendBlobLogFile() error {
	var err error
	// Authenticate using an Azure VM provided managed identity, this will fail if the VM is not assigned a managed identity
	if f.cred == nil {
		f.cred, err = azidentity.NewDefaultAzureCredential(nil)
		// if err != nil {
		// 	f.Log.Debugf("Unable to get a default identity credential: %v", err)
		// 	f.cred, err = azidentity.NewManagedIdentityCredential(nil)
		// 	if err != nil {
		// 		f.Log.Debugf("Unable to get a managed identity credential: %v", err)
		// 		return err
		// 	}
		// }
		if err != nil {
			return err
		}
	}

	if f.appendBlobOutputNum == -1 {
		// On the first write do an extensive search for the latest append blob:
		return f.findLatestAppendBlob()
	} else {
		f.buildBlobURL()
		f.Log.Warnf("createAppendBlobLogFile called with initialized appendBlobOutputNum: %d, attempting to use current output number", f.appendBlobOutputNum)
		return f.createAppendBlobClient(false)
	}
}

func (f *AzureAppendBlob) WriteToAppendBlob(b ByteReadSeekCloser) error {
	var err error
	// Write to the append blob, if the target is sealed or at the maximum block counts increment to the next blob name:
	ctx := context.Background()
	_, err = f.client.AppendBlock(ctx, &b, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlockCountExceedsLimit, bloberror.BlobNotFound, bloberror.InvalidOperation) {
			// BlockCountExceedsLimit - We seem to have reached the maximum number of blocks try the next blob name
			// BlobNotFound - The blob has been deleted, try next blob name
			// InvalidOperation - The blob is likely sealed, try next blob name
			f.appendBlobOutputNum++
			f.appendBlobRetryCount = 0
			f.Log.Warnf("Unable to append to current output, moving to output %d. Error was %v", f.appendBlobOutputNum, err)
			f.buildBlobURL()
			f.createAppendBlobClient(true)
			return f.WriteToAppendBlob(b)
		} else {
			f.appendBlobOutputNum++
			f.appendBlobRetryCount++
			if f.appendBlobRetryCount < appendBlobWriteRetryLimit {
				// Retry the write
				f.Log.Warnf("Failed to write to append blob, attempt %d. Error was %v", f.appendBlobRetryCount, err)
				return f.WriteToAppendBlob(b)
			} else {
				// Fatal error:
				f.Log.Errorf("Failed to write to append blob, attempted %d times: %v", f.appendBlobRetryCount, err)
				return err
			}
		}
	} else {
		f.appendBlobRetryCount = 0
	}
	return err
}

// Convenience wrapper around bytes.Reader to implement io.ReadSeekCloser for AppendBlock writes:
type ByteReadSeekCloser struct {
	*bytes.Reader
}

func NewByteReadSeekCloser(b []byte) ByteReadSeekCloser {
	return ByteReadSeekCloser{bytes.NewReader(b)}
}

func (b *ByteReadSeekCloser) Read(p []byte) (n int, err error) {
	return b.Reader.Read(p)
}

func (b *ByteReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return b.Reader.Seek(offset, whence)
}

func (b *ByteReadSeekCloser) Close() error {
	return nil // No op as there's nothing to close
}
