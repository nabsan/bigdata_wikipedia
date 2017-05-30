package wikipedia

import java.io.File

object WikipediaData {

  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat")
    //if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    //println("resource:"+resource.toURI())
    //if (resource != null) {
       new File(resource.toURI).getPath
    //}else{
      //courseraにsubmitするときはちゃんと↑行に戻すこと。でないと怒られる！ Eclipseの設定でlibrryの外部フォルダの追加でresourcesを選んだらうまくいった！
    //   new File("/Users/manabu/Desktop/spark_class/wikipedia/src/main/resources/wikipedia/wikipedia.dat").getPath
    //}
  }

  private[wikipedia] def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
  }
  
}
