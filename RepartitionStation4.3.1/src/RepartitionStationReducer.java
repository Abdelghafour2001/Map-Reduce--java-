import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RepartitionStationReducer
        extends Reducer<Text, DoubleWritable, Text, IntWritable>
{
    private Text cleS;
    private IntWritable valeurS = new IntWritable();

    @Override
    public void reduce(Text cleI, Iterable<DoubleWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {   // définir la clé de sortie
        cleS = cleI;
        // calculer la valeur de sortie
       int resultat = 0;
        for (DoubleWritable valeurI : valeursI) {
            resultat = resultat + 1;}
        valeurS.set(resultat);
        // émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
    }
}
