import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JMenu;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.UIManager;

import java.awt.BorderLayout;
import javax.swing.Box;
import javax.swing.JLabel;
import javax.swing.JTextField;
import java.awt.Component;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.GridLayout;
import javax.swing.JPasswordField;
import javax.swing.JFormattedTextField;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class CrawlerWindow {

	private JFrame frame;
	private JTextField fldUrl;
	private JTextField fldUsername;
	private JPasswordField fldPassword;
	private JTextField fldMaxUrls;
	private JTextField fldDomain;
	private JTextField fldRoot;
	private JSpinner spnNumThreads;
	
	private Properties props;
	private String origDbUrl;
	private String origUsername;
	private String origPassword;
	private int origMaxUrls;
	private String origDomain;
	private String origRoot;
	private int origNumThreads;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					UIManager.setLookAndFeel(
				            UIManager.getSystemLookAndFeelClassName());
					CrawlerWindow window = new CrawlerWindow();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public CrawlerWindow() {
		initialize();
		readProperties();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 450, 300);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		JMenuBar menuBar = new JMenuBar();
		frame.setJMenuBar(menuBar);
		
		JButton btnRunCrawler = new JButton("Run Crawler");
		btnRunCrawler.addMouseListener(new ButtonClickListener());
		menuBar.add(btnRunCrawler);
		
		JButton btnResume = new JButton("Resume (0)");
		menuBar.add(btnResume);
		
		JButton btnSaveToFile = new JButton("Save To File ");
		menuBar.add(btnSaveToFile);
		
		JButton btnRestoreDefaults = new JButton("Restore Defaults");
		menuBar.add(btnRestoreDefaults);
		frame.getContentPane().setLayout(new GridLayout(0, 2, 0, 5));
		
		JLabel lblDatabaseUrl = new JLabel("Database Url:");
		frame.getContentPane().add(lblDatabaseUrl);
		
		fldUrl = new JTextField();
		frame.getContentPane().add(fldUrl);
		fldUrl.setColumns(10);
		
		JLabel lblUsername = new JLabel("Username:");
		frame.getContentPane().add(lblUsername);
		
		fldUsername = new JTextField();
		frame.getContentPane().add(fldUsername);
		fldUsername.setColumns(10);
		
		JLabel lblPassword = new JLabel("Password:");
		frame.getContentPane().add(lblPassword);
		
		fldPassword = new JPasswordField();
		frame.getContentPane().add(fldPassword);
		
		JLabel lblMaxUrls = new JLabel("Max Urls:");
		frame.getContentPane().add(lblMaxUrls);
		
		fldMaxUrls = new JTextField();
		frame.getContentPane().add(fldMaxUrls);
		fldMaxUrls.setColumns(10);
		
		JLabel lblDomain = new JLabel("Domain:");
		frame.getContentPane().add(lblDomain);
		
		fldDomain = new JTextField();
		frame.getContentPane().add(fldDomain);
		fldDomain.setColumns(10);
		
		JLabel lblRoot = new JLabel("Root:");
		frame.getContentPane().add(lblRoot);
		
		fldRoot = new JTextField();
		frame.getContentPane().add(fldRoot);
		fldRoot.setColumns(10);
		
		JLabel lblNumberOfThreads = new JLabel("Number of Threads:");
		frame.getContentPane().add(lblNumberOfThreads);
		
		spnNumThreads = new JSpinner();
		spnNumThreads.setModel(new SpinnerNumberModel(new Integer(1), new Integer(1), null, new Integer(1)));
		frame.getContentPane().add(spnNumThreads);
	}
	
	public void readProperties() {
  		props = new Properties();
  		try{
	  		FileInputStream in = new FileInputStream("database.properties");
	  		props.load(in);
	  		in.close();
  		}
  		catch(IOException e){}

  		origMaxUrls = Integer.parseInt(props.getProperty("crawler.maxurls"));
  		origDomain = props.getProperty("crawler.domain");
  		origDbUrl = props.getProperty("jdbc.url");
  		origUsername = props.getProperty("jdbc.username");
  		origPassword = props.getProperty("jdbc.password");
  		origRoot = props.getProperty("crawler.root");
  		origNumThreads = Integer.parseInt(props.getProperty("crawler.numthreads"));
  		
  		fldUrl.setText(origDbUrl);
  		fldUsername.setText(origUsername);
  		fldPassword.setText(origPassword);
  		fldMaxUrls.setText(String.valueOf(origMaxUrls));
  		fldDomain.setText(origDomain);
  		fldRoot.setText(origRoot);
  		spnNumThreads.setValue(origNumThreads);
	}
	
	private class ButtonClickListener implements MouseListener{
		@Override
		public void mouseClicked(MouseEvent arg0) {
			Crawler crawler = new Crawler();
			
			long startTime = System.currentTimeMillis();
			crawler.crawl();
			long totalTime = System.currentTimeMillis()-startTime;
			System.out.println("Finished in "+(totalTime/1000f)+" seconds");
			System.out.println("Each url averaged "+(totalTime/1000f/crawler.maxUrls)+" seconds");
			
		}

		@Override
		public void mouseEntered(MouseEvent arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void mouseExited(MouseEvent arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void mousePressed(MouseEvent arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void mouseReleased(MouseEvent arg0) {
			// TODO Auto-generated method stub
			
		}
	}

}
