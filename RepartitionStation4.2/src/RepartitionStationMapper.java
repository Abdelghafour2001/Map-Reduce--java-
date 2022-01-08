import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RepartitionStationMapper
        extends Mapper<LongWritable, Text, Text, Text>
{
    private Text cleI = new Text();
    private Text valeurI = new Text();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
        try {
        while (cleE.get()<1001L){return;}
            String ligne = valeurE.toString();    
           Station.fromLine(ligne);
           if(Station.getAltitude()<0){
           cleI.set(Station.getPays());
           valeurI.set("sud");
           }
            
            context.write(cleI, valeurI);
        } catch (Exception e) {
            // ignorer la donnée d'entrée
            // e.printStackTrace();
        }
    }
}
