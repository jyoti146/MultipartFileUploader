// See https://aka.ms/new-console-template for more information
using MultipartFileUploader;

var uploader = new FileUploader();
var result = await uploader.UploadFile();
