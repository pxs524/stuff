package sparky.app;

import wordcount.WordCount;

public class SparkyApp {
	public static void main(String[] args) {
		WordCount wc = new WordCount();
		wc.run("src/main/resources/randomnotes.txt");
	}
}
