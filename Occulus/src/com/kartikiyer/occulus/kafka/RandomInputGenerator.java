package com.kartikiyer.occulus.kafka;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.log4j.Logger;


public class RandomInputGenerator
{

	Logger log = Logger.getLogger(RandomInputGenerator.class);

	public OutputStream getSocketOutPutStream() throws IOException
	{
		ServerSocket server = new ServerSocket(12181);
		Socket socket = server.accept();
		return socket.getOutputStream();
	}

	public String getRandomInput()
	{
		String[] characters = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };

		int index = (int) (Math.random() * 10);

		int key = (int) (Math.random() * 10) - index;

		if (key == 0)
			key = 1;
		else if (key < 0)
			key = -key;

		return (key + "	" + characters[index] + "\n");

	}

	public void produceInfiniteStream(int delay, OutputStream outStream)
	{
		new Thread(() ->
		{
			// work around for using the variable as candidate for try with rsc ..
			try (
				OutputStream targetOutStream = outStream)
			{
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(targetOutStream));

				while (true)
				{

					bw.write(getRandomInput());
					bw.flush();

					Thread.sleep(delay);
				}
			}
			catch (IOException | InterruptedException e)
			{
				e.printStackTrace();
			}
		}).start();
	}


	public void consumeMessages()
	{
		new Thread(() ->
		{
			try
			{
				System.out.println("1");
				Socket s = new Socket("10.30.7.153", 12181);
				System.out.println("2");

				while (true)
				{
					BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
					// System.out.println("#");
					if (reader.ready())
					{
						// System.out.println(reader.ready());
						System.out.println(reader.readLine());
					}
				}
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}).start();
	}

	public static void main(String[] args) throws IOException, InterruptedException
	{
		RandomInputGenerator producer = new RandomInputGenerator();
		producer.produceInfiniteStream(100, producer.getSocketOutPutStream());
		Thread.sleep(5000);
		// new SocketStreamProducer().consumeMessages();
	}
}
