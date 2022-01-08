import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RepartitionStationReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{ 
	public static double maxx = 0;
    private Text cleS;
    private DoubleWritable valeurS = new DoubleWritable();
    private Text cleF= new Text();
    @Override
    public void reduce(Text cleI, Iterable<DoubleWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {  cleS=cleI;
    while (context.nextKey()){
    
       for (DoubleWritable valeurI : valeursI) {
       if(valeurI.get()>maxx){
      cleF.set(cleS);
       maxx=valeurI.get();       
       valeurS.set(maxx);            
       }}}
            context.write(cleF, valeurS);

    }
}




















































































//credit abdelghafour
