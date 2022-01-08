import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RepartitionStationMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private Text cleI = new Text();
    private DoubleWritable valeurI = new DoubleWritable();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
            if(cleE.get() == 0L) return;
            String ligne = valeurE.toString();    
           Station.fromLine(ligne);
          	cleI.set(Station.getPaay());
            valeurI.set(Station.getCount());
            context.write(cleI, valeurI);
    
    }
}
