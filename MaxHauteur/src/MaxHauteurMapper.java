import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxHauteurMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text cleI = new Text();
    private IntWritable valeurI = new IntWritable();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
         try {
            String ligne = valeurE.toString();
            if(cleE.get() == 0L) return;
	    Arbre.fromLine(ligne);
            cleI.set(Arbre.getGenre());
            valeurI.set(Arbre.getHauteur());
            context.write(cleI, valeurI);
        } catch (Exception e) {
            // ignorer la donnée d'entrée
            // e.printStackTrace();
        }
    }
}
