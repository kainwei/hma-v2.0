/**
 * 
 */
package hma.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author guoyezhi
 *
 */
public class HadoopEmptyFileGenerator extends Configured {
	
	int highDirBeginID = 0;
	
	int midDirBeginID = 0;
	
	int lowFileBeginID = 0;
	
	long fileMaxNum = 0;
	
	public HadoopEmptyFileGenerator() {
		super(new Configuration());
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		HadoopEmptyFileGenerator gen = 
			new HadoopEmptyFileGenerator();
		
		// File Path: /high_dir/mid_dir/low_file
		if (args.length >= 4) {
			gen.highDirBeginID = Integer.parseInt(args[0]);
			gen.midDirBeginID = Integer.parseInt(args[1]);
			gen.lowFileBeginID = Integer.parseInt(args[2]);
			gen.fileMaxNum = Long.parseLong(args[3]);
		} else {
			gen.highDirBeginID = 0;
			gen.midDirBeginID = 0;
			gen.lowFileBeginID = 0;
			gen.fileMaxNum = 120000000;
		}
		
		FileSystem srcFs = FileSystem.get(gen.getConf());
		
		long cnt = 0;
		for (int i = gen.highDirBeginID; i < 9999; i++) {
			for (int j = gen.midDirBeginID; j < 1000; j++) {
				for (int k = gen.lowFileBeginID; k < 1000; k++) {
					
					if (++cnt > gen.fileMaxNum)
						break;
					
					String src = "/" + i 
						+ "/" + j
						+ "/" + k;
					Path f = new Path(src);
					FSDataOutputStream out = srcFs.create(f);
				    out.close();
				}
			}
		}
		
	}

}
