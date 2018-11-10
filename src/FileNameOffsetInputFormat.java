import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
public class FileNameOffsetInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        FileNameOffsetRecordReader fnrr = new FileNameOffsetRecordReader();
        try {
            fnrr.initialize(split, context);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        return fnrr;
    }
}