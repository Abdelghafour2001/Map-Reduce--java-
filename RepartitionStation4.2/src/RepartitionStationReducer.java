import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RepartitionStationReducer
        extends Reducer<Text, Text, Text, Text>
{
    private Text cleS;
    private Text valeurS = new Text();

    @Override
    public void reduce(Text cleI, Iterable<Text> valeursI, Context context)
            throws IOException, InterruptedException
    {   // définir la clé de sortie
        cleS = cleI;
        // calculer la valeur de sortie
        valeurS.set("SUD");
        // émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
    }
}
