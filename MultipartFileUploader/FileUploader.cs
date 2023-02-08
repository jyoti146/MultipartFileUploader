using Amazon.S3;
using Amazon.S3.Model;
using System.Net;

namespace MultipartFileUploader
{
    /// <summary>
    /// It performs multipart uploading of files to AWS S3 bucket
    /// </summary>
    public class FileUploader
    {
        private readonly HttpClient httpClient;
        private string? uploadId;
        
        private static string fileName = "your-file-name";
        private readonly string bucketName = "your-bucket-name";
        private readonly string accessKey = "your-access-key";
        private readonly string secretKey = "your-secret-key";
        private readonly string filePath = "your-file-path";

        private readonly IAmazonS3 s3Client;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileUploader"/> class
        /// </summary>
        /// <param name="baseUrl">Base URL of the APIs</param>
        public FileUploader()
        {
            httpClient = new HttpClient();
            s3Client = new AmazonS3Client(accessKey, secretKey);
        }

        /// <summary>
        /// Divide file into parts and upload each part to S3
        /// </summary>
        /// <returns>Boolean value</returns>
        public async Task<bool> UploadFile()
        {
            try
            {
                // Initiate multipart upload operation 
                var initiateMultipartUploadResponse = await InitiateMultipartUpload();
                if (initiateMultipartUploadResponse == null)
                {
                    return false;
                }
                else
                {
                    uploadId = initiateMultipartUploadResponse.UploadId;
                }

                // Get file info
                var fileInfo = new FileInfo(filePath);
                fileName = fileInfo.Name;
                // Size of the file in bytes
                var maxSize = fileInfo.Length;
                // Number of parts in which file is to be divided
                var numberOfParts = maxSize / 5242880;
                // Size of each file part 
                var partSize = (int)maxSize / (int)numberOfParts;
                int partNumber = 0;
                var parts = new List<PartETag>();

                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    var bytesRemaining = stream.Length;
                    while (bytesRemaining > 0)
                    {
                        int size = Math.Min((int)bytesRemaining, partSize);
                        byte[] buffer = new byte[size];
                        int bytesRead = stream.Read(buffer, 0, size);

                        partNumber += 1;
                        // Upload each file part
                        UploadPartResponse uploadPartResponse = await UploadPartAsync(partNumber, size, buffer);
                        // Get eTag from upload response
                        var eTag = uploadPartResponse.ETag;

                        if (eTag == null)
                        {
                            AbortMultipartUpload();
                            return false;
                        }
                        // Adds eTag and part number of the uploaded file part to the list
                        parts.Add(new PartETag { ETag = eTag, PartNumber = partNumber });

                        bytesRemaining -= bytesRead;
                    }
                }

                // Complete multipart upload operation
                var response = await CompleteMultipartUpload(new CompleteMultipartUploadRequest
                {
                    UploadId = uploadId,
                    Key = fileName,
                    PartETags = parts,
                    BucketName = bucketName
                });

                return response;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Divide file into parts and upload each part to S3 using presigned URL of each part
        /// </summary>
        /// <returns>Boolean value</returns>
        public async Task<bool> UploadFileUsingPresignedUrl()
        {
            try
            {
                // Initiate multipart upload operation 
                var initiateMultipartUploadResponse = await InitiateMultipartUpload();
                if (initiateMultipartUploadResponse == null)
                {
                    return false;
                }
                else
                {
                    uploadId = initiateMultipartUploadResponse.UploadId;
                }

                // Get file info
                var fileInfo = new FileInfo(filePath);
                fileName = fileInfo.Name;
                // Size of the file in bytes
                var maxSize = fileInfo.Length;
                // Number of parts in which file is to be divided
                var numberOfParts = maxSize / 5242880;
                // Size of each file part 
                var partSize = (int)maxSize / (int)numberOfParts;
                int partNumber = 0;
                var parts = new List<PartETag>();

                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    var bytesRemaining = stream.Length;
                    while (bytesRemaining > 0)
                    {
                        int size = Math.Min((int)bytesRemaining, partSize);
                        byte[] buffer = new byte[size];
                        int bytesRead = stream.Read(buffer, 0, size);

                        partNumber += 1;

                        // Get presigned URL for the file part
                        string presignedUrl = GetPreSignedUrl(partNumber);
                        // Upload the file part using presigned URL
                        var eTag = await UploadPartUsingPresignedUrl(presignedUrl, buffer);

                        if (eTag == null)
                        {
                            AbortMultipartUpload();
                            return false;
                        }

                        // Adds eTag and part number of the uploaded file part to the list
                        parts.Add(new PartETag { ETag = eTag, PartNumber = partNumber });

                        bytesRemaining -= bytesRead;
                    }
                }

                // Complete multipart upload operation
                var response = await CompleteMultipartUpload(new CompleteMultipartUploadRequest
                {
                    UploadId = uploadId,
                    Key = fileName,
                    PartETags = parts,
                    BucketName = bucketName
                });

                return response;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Initiate multipart upload operation
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        private async Task<InitiateMultipartUploadResponse> InitiateMultipartUpload()
        {
            var response = await s3Client.InitiateMultipartUploadAsync(bucketName, fileName);
            return response;
        }

        /// <summary>
        /// Get presigned URL of the file for upload
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="partNumber"></param>
        /// <param name="uploadId"></param>
        /// <returns></returns>
        private string GetPreSignedUrl(int partNumber)
        {
            var request = new GetPreSignedUrlRequest
            {
                BucketName = bucketName,
                Key = fileName,
                PartNumber = partNumber,
                UploadId = uploadId,
                Verb = HttpVerb.PUT,
                Expires = DateTime.UtcNow.AddHours(12)
            };

            string url = s3Client.GetPreSignedURL(request);

            return url;
        }

        /// <summary>
        /// Upload part of the file using presigned URL
        /// </summary>
        /// <param name="presignedUrl"></param>
        /// <param name="part"></param>
        /// <returns></returns>
        private async Task<string?> UploadPartUsingPresignedUrl(string presignedUrl, byte[] part)
        {
            var response = await httpClient.PutAsync(presignedUrl, new StreamContent(new MemoryStream(part)));

            if (response.StatusCode == HttpStatusCode.OK)
            {
                var eTag = response.Headers.AsQueryable().FirstOrDefault(x => x.Key == "ETag").Value.First();
                return eTag;
            }
            else
            {
                return null;
            }
        }

        private async Task<UploadPartResponse> UploadPartAsync(int partNumber, int size, byte[] part)
        {
            var uploadPartRequest = new UploadPartRequest
            {
                BucketName = bucketName,
                Key = fileName,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = size,
                InputStream = new MemoryStream(part)
            };
            UploadPartResponse uploadPartResponse = await s3Client.UploadPartAsync(uploadPartRequest);

            return uploadPartResponse;
        }

        /// <summary>
        /// Complete multipart upload operation
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        private async Task<bool> CompleteMultipartUpload(CompleteMultipartUploadRequest request)
        {
            var response = await s3Client.CompleteMultipartUploadAsync(request);

            if (response.HttpStatusCode == HttpStatusCode.OK)
            {
                return true;
            }
            else
            {
                AbortMultipartUpload();
                return false;
            }
        }

        /// <summary>
        /// Abort multipart upload operation
        /// </summary>
        private async void AbortMultipartUpload()
        {
            var request = new AbortMultipartUploadRequest { Key = fileName, UploadId = uploadId, BucketName = bucketName };
            var response = await s3Client.AbortMultipartUploadAsync(request);
        }
    }
}
