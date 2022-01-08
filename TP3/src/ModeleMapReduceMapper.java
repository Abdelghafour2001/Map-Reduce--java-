import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ModeleMapReduceMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text cleI = new Text();
    private IntWritable valeurI = new IntWritable();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
        try {

            // données d'entrée provenant des fichiers à traiter
            String ligne = valeurE.toString();

            // définir la clé de sortie
            cleI.set(ligne.substring(0, 5));

            // calculer la valeur de sortie
            valeurI.set(ligne.length());

            // émettre une paire (clé, valeur)
            context.write(cleI, valeurI);

        } catch (Exception e) {
            // ignorer la donnée d'entrée
            // e.printStackTrace();
        }
    }
}
