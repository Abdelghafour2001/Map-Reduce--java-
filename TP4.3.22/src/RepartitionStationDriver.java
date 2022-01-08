import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RepartitionStationDriver
    extends Configured
    implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        // temps de démarrage
        Long initTime = System.currentTimeMillis();

        // vérifier les paramètres
        if (args.length != 2) {
            System.err.println("Usage: RepartitionStationDriver <inputpath> <outputpath>");
            System.exit(-1);
        }

        // créer le job map-reduce
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "RepartitionStation Job");
        job.setJarByClass(RepartitionStationDriver.class);

        // définir la classe Mapper et la classe Reducer
        job.setMapperClass(RepartitionStationMapper.class);
        /*job.setCombinerClass(RepartitionStationCombiner.class);*/
        job.setReducerClass(RepartitionStationReducer.class);

        // définir les données d'entrée : TextInputFormat => clés=LongWritable, valeurs=Text
        FileInputFormat.setInputDirRecursive(job, false);       // mettre true si les fichiers sont dans des sous-dossiers
        FileInputFormat.addInputPath(job, new Path(args[0]));   /** VOIR CONFIG ENTREES DANS LE MAKEFILE **/
        job.setInputFormatClass(TextInputFormat.class);

        // sorties du mapper = entrées du reducer et entrées et sorties du combiner
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // définir les données de sortie : dossier et types des clés et valeurs
        FileOutputFormat.setOutputPath(job, new Path(args[1])); /** VOIR CONFIG SORTIE DANS LE MAKEFILE **/
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // lancer le job et attendre sa fin
        Long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        Long endTime = System.currentTimeMillis();

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        // préparer et lancer un job
        RepartitionStationDriver driver = new RepartitionStationDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}
