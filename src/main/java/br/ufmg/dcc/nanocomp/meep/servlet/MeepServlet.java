package br.ufmg.dcc.nanocomp.meep.servlet;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/process")
public class MeepServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	private static final AtomicInteger THREAD_ID = new AtomicInteger(0);
	
	@Override
	protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		File dir = null;
		try {
			dir = File.createTempFile("meepserver", "");
			dir.delete();
			if(!dir.mkdir()) throw new IOException("Failed to create file");
			
			Process process = Runtime.getRuntime().exec("meep", null, dir);
			
			Thread errorWatcher = new Thread(()->{
				try(Scanner scanner = new Scanner(process.getErrorStream())) {
					while(scanner.hasNextLine()) {
						System.err.println(scanner.nextLine());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			},"process-error-watcher-"+THREAD_ID.incrementAndGet());

			Thread outputWatcher = new Thread(()->{
				try(Scanner scanner = new Scanner(process.getInputStream())){
					while(scanner.hasNextLine()) {
						resp.getWriter().println(scanner.nextLine());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			},"process-output-watcher-"+THREAD_ID.incrementAndGet());

			outputWatcher.start();
			errorWatcher.start();
			
			try (OutputStream os = process.getOutputStream();
					OutputStreamWriter writer = new OutputStreamWriter(os)){
				String line;
				while((line=req.getReader().readLine())!=null) {
					writer.write(line);
					writer.write("\n");
				}
			}
			
			try {		
				outputWatcher.join(30*60*1000); //Waits tops 1 h
				errorWatcher.join(60*1000);
				process.waitFor(1, TimeUnit.MINUTES);
			} finally {
				process.destroyForcibly();
			}
			
		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			e.printStackTrace();
		} finally {
			if(dir!=null && dir.exists()) {
				for(File f : dir.listFiles()){
					f.delete();
				}
				dir.delete();
			}
		}
	}

}
