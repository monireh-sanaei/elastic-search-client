package elasticsearchClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.XML;


/* This class processes XML and JSON files, including reading, writing and parsing*/
public class FileManager {

	
	String xmlDirPath = "C:/Users/Monireh/Documents/Imaginatio-project/Files";
	String jsonDirPath = "C:/Users/Monireh/Documents/Imaginatio-project/JsonFiles"; // Using UTF-8 encoding
																					
	ArrayList <JSONObject> notice_List;
	

	/***********************/
	public static void main(String[] args) {

		FileManager fileManager = new FileManager();
//		fileManager.readXMLFiles();
//		fileManager.parseJson();
		
		//Test
		File[] files = fileManager.listFiles("C:/Users/Monireh/Documents/Imaginatio-project/JsonFiles");
		System.out.println(files[106].getName());
	}

	/***********************/
	public FileManager() {
		
		notice_List = new ArrayList <JSONObject>();
	}

	/***********************/
	public void readXMLFiles() {

		File[] files = listFiles(xmlDirPath);

		if (files != null && files.length != 0) {

			for (File file : files) {

				String fileName = file.getName();

				if (fileName.endsWith("xml"))
					XMLToJson(file);
			}
		}
	}

	/***********************/
	public File[] listFiles(String dirPath) {

		File dir = new File(dirPath);

		File[] files = null;

			if (dir.isDirectory()) {
				files = dir.listFiles();
			} else {
				System.out.println("This path is not a directory");
			}
		return files;
	}

	/***********************/
	public void XMLToJson(File xmlfile) {

		String fileName = xmlfile.getName();
		String newName = fileName.substring(0, fileName.length() - 4).concat(".json");
		File jsonFile = new File(jsonDirPath + "/" + newName);

		StringBuffer xmlString = new StringBuffer();

		// Read the XML file
		xmlString = readTextFile(xmlfile, "UTF-8");

		// Convert the text from XML file to a JSon object
		JSONObject jsonObject = null;
		try {
			jsonObject = XML.toJSONObject(xmlString.toString());
		} 
		catch (JSONException e) {
			System.out.println("Exception in converting " + fileName + " to JSON object");
			e.printStackTrace();
		}

		// Write the JSon object to a new file (.json)
		writeTextFile(jsonObject.toString(4), jsonFile);
	}

	/***********************/
	public StringBuffer readTextFile(File textfile, String charset) {

		Path filePath = textfile.toPath();

		StringBuffer content = new StringBuffer();

		BufferedReader reader = null;
		try {
			reader = Files.newBufferedReader(filePath, Charset.forName(charset));

			String line = null;
			while ((line = reader.readLine()) != null) {
				content.append(line);
			}
		}
		catch (IOException x) {
			System.out.println("Exception in reading " + textfile.getName());
			System.err.format("IOException: %s%n", x);
		}
		finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		return content;
	}

	/***********************/
	public void writeTextFile(String content, File destFile) {

		String fileName = destFile.getName();

		BufferedWriter writer = null;
		try {
			writer = Files.newBufferedWriter(destFile.toPath(), Charset.forName("UTF-8"));
			writer.write(content);
		}
		catch (IOException e) {
			System.out.println("Exception in writing " + fileName);
			e.printStackTrace();
		}
		finally {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					System.out.println(e.getMessage());
				}
			}
		}
	}

	/***********************/
	public ArrayList<JSONObject> parseJson() {

		File[] files = listFiles(jsonDirPath);

		if (files != null && files.length != 0) {
			
			System.out.println(files.length + " files :");
			
			int n = 0;
			for (File jsonFile : files) {

				System.out.println(++n + " : Parsing " + jsonFile.getName());

				StringBuffer jsonText = readTextFile(jsonFile, "UTF-8");

				JSONTokener jsonTokener = new JSONTokener(jsonText.toString());
				JSONObject jsonObject = new JSONObject(jsonTokener);

				JSONObject noticeObj = jsonObject.getJSONObject("NOTICE");

				JSONObject workObj = noticeObj.getJSONObject("WORK");

				Object work_has_expObject = workObj.get("WORK_HAS_EXPRESSION");
				
				
				JSONArray work_has_expArr = null;
				JSONObject work_has_expObj = null;
				
				if(work_has_expObject instanceof JSONArray) {					
					work_has_expArr = workObj.getJSONArray("WORK_HAS_EXPRESSION");
					
					// Modify the WORK_HAS_EXPRESSION JSONArray to include only the French parts
					int length_array = work_has_expArr.length();
					int index = 0;
					while (length_array > index) {

						JSONObject work_has_exp = work_has_expArr.getJSONObject(index);

						JSONObject embedded_Notice = work_has_exp
								.getJSONObject("EMBEDDED_NOTICE");
						JSONObject expression = embedded_Notice
								.getJSONObject("EXPRESSION");
						JSONObject exp_use_lang = expression
								.getJSONObject("EXPRESSION_USES_LANGUAGE");
						String prefLabel = exp_use_lang.getString("PREFLABEL");

						if (!prefLabel.equals("français")) {
							work_has_expArr.remove(index);
							length_array--;
						} else {
							index++;
						}
					}
				}
				else {
					work_has_expObj = workObj.getJSONObject("WORK_HAS_EXPRESSION");
					
					JSONObject embedded_Notice = work_has_expObj
							.getJSONObject("EMBEDDED_NOTICE");
					JSONObject expression = embedded_Notice
							.getJSONObject("EXPRESSION");
					JSONObject exp_use_lang = expression
							.getJSONObject("EXPRESSION_USES_LANGUAGE");
					String prefLabel = exp_use_lang.getString("PREFLABEL");

					if (!prefLabel.equals("français")) {
						workObj.remove("WORK_HAS_EXPRESSION");
					}						
				}				

				notice_List.add(noticeObj);
			}
		}
		return notice_List;
	}

}
