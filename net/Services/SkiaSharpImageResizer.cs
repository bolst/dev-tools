using SkiaSharp;

// namespace ...;

public class SkiaSharpImageResizer
{
    private const float MIN_SCALE_FACTOR = 0.5f; // do not scale below 50% original size
    private const float SCALE_DEC = 0.1f; // reduce scaling by 10% in each iteration
    
    private const int INIT_QUALITY = 90; // start with 90% quality
    private const int MIN_QUALITY = 50; // do not go below 50% quality
    private const int QUALITY_STEP = 10; // reduce quality by 10% in each iteration


    public async Task<byte[]> ApproxResizeImageAsync(Stream imageStream, long targetSizeKB = 500)
    {
        using var memoryStream = new MemoryStream();
        await imageStream.CopyToAsync(memoryStream);
        byte[] originalBytes = memoryStream.ToArray();
        
        // if image is alr smaller than target size, return
        if (originalBytes.Length <= targetSizeKB * 1024)
            return originalBytes;
        
        using var originalBitmap = SKBitmap.Decode(new MemoryStream(originalBytes));
        var quality = INIT_QUALITY; // start with high quality
        var scaleFactor = 1.0f; // start with no scaling

        long imageSizeBytes;
        byte[] imageBytes;

        do
        {
            var resizedBitmap = scaleFactor < 1.0f ? ResizeBitmap(originalBitmap, scaleFactor) : originalBitmap;
            
            imageBytes = BitmapToByteArray(resizedBitmap, quality);
            imageSizeBytes = imageBytes.Length;

            if (imageSizeBytes > targetSizeKB * 1024 && quality > MIN_QUALITY)
            {
                quality -= QUALITY_STEP;
            }
            else if (imageSizeBytes < targetSizeKB * 1024 / 2 && scaleFactor > MIN_SCALE_FACTOR)
            {
                scaleFactor -= SCALE_DEC;
            }
            else
            {
                break;
            }

        } while (imageSizeBytes > targetSizeKB * 1024 || imageSizeBytes < targetSizeKB * 1024 / 2);

        return imageBytes;
    }



    private SKBitmap ResizeBitmap(SKBitmap originalBitmap, float scaleFactor) 
        => originalBitmap
            .Resize(
                new SKImageInfo((int)(originalBitmap.Width * scaleFactor), (int)(originalBitmap.Height * scaleFactor)), 
                new SKSamplingOptions(SKCubicResampler.Mitchell)
                );

    private byte[] BitmapToByteArray(SKBitmap bitmap, int quality)
    {
        using var image = SKImage.FromBitmap(bitmap);
        using var data = image.Encode(SKEncodedImageFormat.Jpeg, quality);
        return data.ToArray();
    }
    
}