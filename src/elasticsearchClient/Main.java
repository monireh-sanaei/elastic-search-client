package elasticsearchClient;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;


public class Main {

	static NodeClient nodeClient;
	
	static Scanner input;
	static Scanner full_text_input = new Scanner(System.in);
	static Scanner exact_value_input = new Scanner(System.in);
	
	/***********************/
	public static void main(String [] args) {
		
		nodeClient = new NodeClient();

		startSearch();
	}
	
	/***********************/
	public static void startSearch() {
		
		int option = 0;
		
		printSearchMenu();
		
		input = new Scanner(System.in);	
		
		while (input.hasNext()) {
			
			option = input.nextInt();
			
			if (option == 1) {
				full_text_search();
			}
			else if (option == 2) {
				exact_value_search();
			}
			else if (option == 3) {
				break;
			}
			else {
				System.out.println("Invalid input");
				continue;
			}
		}
		exit();
	}
	
	/***********************/
	public static void printSearchMenu() {
		
		System.out.println("*****Start to search*****");
		System.out.println("Enter 1 for full_text search and 2 for exact_value search");
		System.out.println("Enter 3 to exit :");
	}
	
	/***********************/
	public static void printFull_TextSearchMenu() {
		
		System.out.println("*****Full text search*****");
		System.out.println("Enter the term(s) to search seperated by ',' and a space in case of phrases, starting with '+' for inclusion and '-' for exclusion");
		System.out.println("Enter 'back' to return to the main search menu :");
	}
	
	/***********************/
	public static void printExact_ValueSearchMenu() {
		
		System.out.println("*****Exact value search*****");
		System.out.println("Enter 1 to search by publication date and 2 to search by identifier"); 
		System.out.println("Enter 3 to return to the main search menu :");
	}
	
	/***********************/
	public static void full_text_search() {
								
		ArrayList <String> include_matches = new ArrayList <String>();
		ArrayList <String> exclude_matches = new ArrayList <String>();
		
		ArrayList <String> include_match_phrases = new ArrayList <String>();
		ArrayList <String> exclude_match_phrases = new ArrayList <String>();
		
		printFull_TextSearchMenu();
		
		while (full_text_input.hasNext()) {
			
			String terms = full_text_input.nextLine();

			if (terms.equals("back")) 
				break;
			
			// Reset the match and match_phrase lists
			include_matches.clear();
			exclude_matches.clear();
			include_match_phrases.clear();
			exclude_match_phrases.clear();			
			
			// Tokenize the input terms
			StringTokenizer tokenizer = new StringTokenizer(terms, ",");			
			
			while (tokenizer.hasMoreTokens()) {
				
				String token = tokenizer.nextToken();
				
				// Include terms
				if (token.startsWith("+")) {
					String include_term = token.substring(1, token.length()); // Remove '+'
					
					// In case of phrase
					if (include_term.contains(" "))
						include_match_phrases.add(include_term);
					else 
						include_matches.add(include_term);
				}
				
				// Exclude terms
				else if (token.startsWith("-")){
					String exclude_term = token.substring(1, token.length()); // Remove '-'
					
					// In case of phrase
					if(exclude_term.contains(" "))
						exclude_match_phrases.add(exclude_term);
					else
						exclude_matches.add(exclude_term);
				}
			
			}
						
			if ((!include_matches.isEmpty() | ! exclude_matches.isEmpty()) && (include_match_phrases.isEmpty() && include_match_phrases.isEmpty())) {
				System.out.println("Calling searchByMatch");
				nodeClient.searchByMatch(include_matches, exclude_matches);	
			}
			if ((!include_match_phrases.isEmpty() | ! exclude_match_phrases.isEmpty()) && (include_matches.isEmpty() && exclude_matches.isEmpty())) {
				System.out.println("Calling searchByMatch_Phrase");
				nodeClient.searchByMatch_Phrase(include_match_phrases, exclude_match_phrases);
			}
			if ((!include_matches.isEmpty() | ! exclude_matches.isEmpty()) && (!include_match_phrases.isEmpty() | ! exclude_match_phrases.isEmpty())) {
				System.out.println("Calling searchFullText");
				nodeClient.searchFullText(include_matches, exclude_matches, include_match_phrases, exclude_match_phrases);
			}
			
			printFull_TextSearchMenu();
		}
		startSearch();

	}	
	
	/***********************/
	public static void exact_value_search() {
				
		String pub_date = "";
		String identifier = "";
				
		printExact_ValueSearchMenu();
		
		while (exact_value_input.hasNext()) {
			
			int option = exact_value_input.nextInt();
					
				if (option == 1) {
					System.out.println("Enter the publication date (yyyy-mm-dd) :");
					
					exact_value_input.nextLine(); // to go to next line for entering the date
					
					pub_date = exact_value_input.nextLine();
					nodeClient.searchByPublication_Date(pub_date);
				}
				
				else if (option == 2) {
					System.out.println("Enter the identifier :");
					
					exact_value_input.nextLine(); // to go to next line for entering the identifier
					
					identifier = exact_value_input.nextLine();
					nodeClient.searchByIdentifier(identifier);
				}
			
			else if (option == 3) {
				break;
			}
			
			else {
				System.out.println("Invalid input");
				continue;
			}
			
			printExact_ValueSearchMenu();
		}
		startSearch();
	}
	
	/***********************/
	public static void exit() {
		
//		input.close();
		
		if (! full_text_input.equals(null))
			full_text_input.close();
		if (! exact_value_input.equals(null))
			exact_value_input.close();
		
		nodeClient.shutdown();
	}
}
