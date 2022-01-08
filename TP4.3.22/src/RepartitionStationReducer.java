import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RepartitionStationReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    private Text cleS;
    private DoubleWritable valeurS = new DoubleWritable();

    @Override
    public void reduce(Text cleI, Iterable<DoubleWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {   // définir la clé de sortie
         cleS = cleI;
        // calculer la valeur de sortie
       double maxx = 0;
       for (DoubleWritable valeurI : valeursI) {
       maxx=valeurI.get();
                   }
           
        valeurS.set(maxx);
        // émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
        
    }
}
