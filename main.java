import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rainfall {
static int j=0;
static int s=0;
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private FloatWritable one = new FloatWritable();
    private Text word = new Text();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
String[] states = new String[28];
    states[4] = "AndraPradesh";
    states[5] = "ArunachalPradesh";
    states[6] = "Assam";
    states[7] = "Bihar";
    states[16] = "Goa";
    states[17] = "Gujarat";
    states[18] = "Haryana";
    states[19] = "HimachalPradesh";
    states[8] = "JammuAndKashmir";
    states[9] = "Jarkhand";
    states[10] = "Karnataka";
    states[11] = "Kerala";
    states[20] = "MadhyaPradesh";
    states[21] = "Maharashtra";
    states[22] = "Manipur";
    states[23] = "Meghalaya";
    states[12] = "Mizoram";
    states[13] = "Nagaland";
    states[14] = "Orissa";
    states[15] = "Punjab";
    states[24] = "Rajasthan";
    states[25] = "Sikkim";
    states[26] = "TamilNadu";
    states[27] = "Tiripura";
    states[0] = "Uttarpradesh";
    states[1] = "Uttarakhand";
    states[2] = "WestBengal";
    states[3] = "Chattisgarh";

float sum=0;
float avg=0;
float loop=0;


float[] arr=new float[5];
float[] nleft=new float[]{0.5f,1.5f,0f,1.5f};
float den=6.0f;
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
String each=new String(word.toString());
float single=Float.parseFloat(each);
sum=sum+single;
arr[(int)loop++]=single;
        //context.write(word, one);
      }
avg=sum/5.0f;
float l=0;
float numer=0;
float nright=0;
float res=0;
float res2=0;
float res3=0;
float[] numerator=new float[5];
for(l=0;l<4;l++)
{
if((arr[(int)l]-avg)>0)
nright=arr[(int)l]-avg;
else
nright=avg-arr[(int)l];
numerator[(int)l]=nright*nleft[(int)l];
}
for(l=0;l<4;l++)
{
numer=numer+numerator[(int)l];
}
 res=numer/den;
 res2=avg-(res*2.5f);

 res3=res2+(res*5.0f);
 if(j==0)
{
word.set(states[s]+"  rainfall");
j=1;
}
else if(j==1)
{
word.set(states[s]+"  riceproduction");
j=2;
}
else if(j==2)
{
word.set(states[s]+"  wheatproduction");
j=3;
}
else if(j==3)
{
word.set(states[s]+"  ricearea");
j=4;
}
else if(j==4)
{
word.set(states[s]+"  wheatarea");
j=5;

}
else if(j==5)
{
word.set(states[s]+"  riceirrigation");
j=6;

}
else if(j==6)
{
word.set(states[s]+"  wheatirrigation");
j=7;

}
else if(j==7)
{
word.set(states[s]+"  gdp");
j=0;
s++;
}

one.set(res3);
context.write(word,one);

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
     

      for (FloatWritable val : values) {
              result.set(val.get());
    	      context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Rainfall.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

