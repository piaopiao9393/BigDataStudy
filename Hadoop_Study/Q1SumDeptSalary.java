import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Parh;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.ipnput.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.ipnput.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q1SumDeptSalary extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		//缓存dept文件中的数据
		private Map<String,String> deptMap = new HashMap<String,String>();
		private String[] kv;

		//setup方法在Map方法执行前执行，并且只执行一次
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			BufferedReader in = null;
			try{
				//从当前作业中回去要缓存的文件
				Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				String deptIdName = null;
				for(Path path:paths){
					if(path.toString().contains("dept")){
						in = new BufferedReader(new FileReader(path.toString()));
						while(null != (deptIdName = in.readLine())){
							//对部门文件字段进行拆分并缓存到deptMap中
							//其中Map中key为部门编号，value为所在部门名称
							deptMap.put(deptIdName.split(",")[0],deptIdName.split(",")[1]);
						}
					}
				}
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				if(in != null){
					in.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{

			//对员工文件字段进行拆分
			kv = value.toString().split(",");
			//map join:在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
			if(deptMap.containsKey(kv[7])){
				if(null != kv[5] && !"".euqals(kv[5].toString())){
					context.write(new Text(dept.get(kv[7].trim())),new Text(kv[5].trim()));
				}
			}
		}
	}
}