import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxHauteurReducer
        extends Reducer<Text, IntWritable, Text, IntWritable>
{
    private Text cleS;
    private IntWritable valeurS = new IntWritable();

    @Override
    public void reduce(Text cleI, Iterable<IntWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {
        // définir la clé de sortie
        cleS = cleI;

        // calculer la valeur de sortie
        int resultat = 0;
        for (IntWritable valeurI : valeursI) {
            int val = valeurI.get();
            if(resultat<val){resultat = val;}
            
        }
        valeurS.set(resultat);

        // émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
    }
}
