package br.ufmg.dcc.nanocomp.meep.websocket;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.ScalarDS;
import ncsa.hdf.object.h5.H5File;

/**
 * A WebSocket for running and receiving meep data
 * @author Jerônimo Nunes Rocha
 *
 */
@ServerEndpoint(value="/execute")
public class Execute {

	/**
	 * Unique ID for threads started by this
	 */
	private static final AtomicInteger THREAD_ID = new AtomicInteger(0);

	/**
	 * Pattern to recognize EZ files outputeb by meep
	 */
	private static final Pattern EZ_FILE_PATTERN = Pattern.compile("creating output file \"(?<filename>.*ez-.*)\"");

	/**
	 * Colors pallet to convert data into colors
	 */
	private static final ByteBuffer[] PALLET;

	/**
	 * The max index of {@link #PALLET} array
	 */
	private static final int PALLET_TOP;

	/**
	 * Meep process, must be running while session is open
	 */
	private Process process;
	
	/**
	 * {@link Writer} for meep input
	 */
	private Writer writer;
	
	/**
	 * Directory where meep will run
	 */
	private File dir;

	/**
	 * Path to EZ files outputed by meep
	 */
	private Queue<String> ezFiles = new LinkedList<>();

	/**
	 * Used to inform processFiles thread that more files could come
	 */
	private boolean running = true;

	static {
		//Loading #PALLET
		try(InputStream is = Execute.class.getResourceAsStream("/pallet.png")) {
			BufferedImage image = ImageIO.read(is);
			PALLET = new ByteBuffer[image.getWidth()];
			PALLET_TOP = PALLET.length-1;
			for(int i = 0; i<image.getWidth(); i++) {
				int rgb = image.getRGB(i, 0);
				PALLET[i] = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(rgb);
				PALLET[i].rewind();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@OnOpen
	public void onOpen(Session session) {
		try {
			dir = File.createTempFile("meepserver", "");
			dir.delete();
			if(!dir.mkdir()) throw new IOException("Failed to create file");
			startMeep(session);
			synchronized(session) {
				session.getBasicRemote().sendText("succ:Sending data to server...");
			}
		} catch (IOException e) {
			try {
				session.getBasicRemote().sendText("error:Failed to start meep");
				session.getBasicRemote().sendText("end");
			} catch (Exception e1) {
				// ignore
			}
		}
	}

	@OnClose
	public void onClose() {
		process.destroyForcibly();
		for(File f : dir.listFiles()) {
			f.delete();
		}
		dir.delete();
	}

	@OnMessage
	public void onMessage(String line, boolean last, Session session) {
		try {
			writer.append(line);
			if(last) {
				writer.append("\n");
				writer.flush();
			}
		} catch (IOException e) {
			try {
				synchronized(session) {
					session.getBasicRemote().sendText("error:Failed to send data to meep");
					session.getBasicRemote().sendText("end");
				}
			} catch (IOException e1) {
				//ignore
			}
		}
	}
	
	private void startMeep(Session session) throws IOException {
		process = Runtime.getRuntime().exec("meep", null, dir);
		writer = new OutputStreamWriter(process.getOutputStream());
		
		Thread errorWatcher = new Thread(()->{
			try(Scanner scanner = new Scanner(process.getErrorStream())) {
				while(scanner.hasNextLine()) {
					synchronized(session) {
						session.getBasicRemote().sendText("error:"+scanner.nextLine());
					}
				}
			} catch (Exception e) {
				//exceptions will make the thread stop and that's ok
			}
		},"execution-error-watcher-"+THREAD_ID.incrementAndGet());

		Thread outputWatcher = new Thread(()->{
			try(Scanner scanner = new Scanner(process.getInputStream())){
				while(scanner.hasNextLine()) {
					String line = scanner.nextLine();
					synchronized(session) {
						session.getBasicRemote().sendText("info:"+line);
					}
					Matcher matcher = EZ_FILE_PATTERN.matcher(line);
					if(matcher.find()) {
						synchronized(ezFiles) {
							ezFiles.offer(new File(dir,matcher.group("filename")).getAbsolutePath());
							ezFiles.notify();
						}
					}
				}
			} catch (Exception e) {
				//exceptions will make the thread stop and that's ok
			} finally {
				synchronized(ezFiles) {
					running = false;
					ezFiles.notify();
				}
			}
		},"execution-output-watcher-"+THREAD_ID.incrementAndGet());

		Thread processFilesThread = new Thread(()->{
			try {
				int h = 0, w = 0; //height and width of images
				double range = 0d; //max absolute value outputed to use to convert data to color
				List<double[]> images = new ArrayList<>(); //Collected data to send
				
				try {
					String file = null;

					synchronized(ezFiles) { //Take the first file
						//If the list have one file, meep could be writing it
						while(running && ezFiles.size()<2) {
							ezFiles.wait();
						}
						file = ezFiles.poll();
					}

					while(file!=null) {
						H5File h5File = new H5File(file,FileFormat.READ);
						ScalarDS scalar =  (ScalarDS) h5File.get("ez");
						double[] data =  (double[]) scalar.getData();
						h = scalar.getHeight();
						w = scalar.getWidth();
						h5File.close();
						images.add(data);
						for(int k = 0;k<data.length;k++) {
							double v = Math.abs(data[k]);
							if(v>range) range = data[k];
						}

						synchronized(ezFiles) { //Take the next file
							//If the list have one file, meep could be writing it
							while(running && ezFiles.size()<2) {
								ezFiles.wait();
							}
							file = ezFiles.poll();
						}
					};
				} catch (Exception e) {
					synchronized(session) {
						session.getBasicRemote().sendText("error:Failed reading meep outputed data");
						session.getBasicRemote().sendText("end");
					}
					e = new IOException("Failed reading meep outputed data",e);
					e.printStackTrace();
					throw e;
				}
				
				double factor = PALLET_TOP/(range*2);
				int o = 1;
				int u = images.size();
				for(double[] data : images) {
					// messages does not need to be synchronized because the other threads have already finished
					session.getBasicRemote().sendText("succ:Sending image "+o+++" of "+u);
					int i,j;
					for(j = w-1; j>0;j--){
						for(i = 0; i < h;i++){
							int index = (int) ((data[i*w+j]+range)*factor);
							index = index<0?0:index>PALLET_TOP?PALLET_TOP:index;
							session.getBasicRemote().sendBinary(PALLET[index],false);
						}
					}
					int k = h-1;
					for(i = 0; i < k;i++){
						int index = (int) ((data[i*w+j]+range)*factor);
						index = index<0?0:index>PALLET_TOP?PALLET_TOP:index;
						session.getBasicRemote().sendBinary(PALLET[index],false);
					}
					int index = (int) ((data[i*w+j]+range)*factor);
					index = index<0?0:index>PALLET_TOP?PALLET_TOP:index;
					session.getBasicRemote().sendBinary(PALLET[index],true);
				}
				session.getBasicRemote().sendText("succ:Execution completed successfully");
				session.getBasicRemote().sendText("end");
			} catch(Exception e) {
				//ignore
			}
		},"execution-process-files-"+THREAD_ID.incrementAndGet());

		Thread timeoutWatcher = new Thread(()->{
			try {
				outputWatcher.join(15*60*1000); //Waits tops 15 min
				errorWatcher.join(60*1000);
				processFilesThread.join(60*1000);
				process.waitFor(1, TimeUnit.MINUTES);
				if(process.isAlive()) {
					synchronized(session) {
						session.getBasicRemote().sendText("error:Timeout, meep running for more than 15 minutes");
						session.getBasicRemote().sendText("end");
					}
				}
			} catch (Exception e) {
				//ignore
			} finally {
				process.destroyForcibly();
			}
		},"execution-timeout-watcher-"+THREAD_ID.incrementAndGet());
		
		outputWatcher.start();
		errorWatcher.start();
		timeoutWatcher.start();
		processFilesThread.start();
	}

}
