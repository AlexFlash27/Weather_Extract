using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Timers;
using System.Web.Script.Serialization;
using MySql.Data.MySqlClient;


namespace ConsoleApp1
{
    public class Jsondata
    {
        public string Server { get; set; }
        public string User { get; set; }
        public string Database { get; set; }
        public string Password { get; set; }
    }
    class Program
    {
        private static void Main(string[] args)

        {
            if (args is null)
            {
                throw new ArgumentNullException(nameof(args));
            }

            Timer timer = new Timer
            {
                //Interval = 60000
                Interval = 1800000
            };
            Console.WriteLine(DateTime.Now);
            timer.Elapsed += Load_to_DB;
            timer.Elapsed += TimerCallback;
            timer.Start();
            Console.ReadLine();
            GC.KeepAlive(timer);
        }
        static void Load_to_DB(object sender, ElapsedEventArgs e)
        {
            //string FilePath = @"\\OBMEN\WeatherLink\Garmony\download.txt"; //localhost
            string FilePath = "D:/WeatherLink/Garmony/download.txt";
            //string OutputhFilePath = "C:/users/user/desktop/download11.txt"; //localhost
            string OutputhFilePath = "C:/Users/Admin/Desktop/weatherextract/download11.txt";

            _ = new List<string>();
            List<string> lines = File.ReadAllLines(FilePath).ToList();

            var sb = new StringBuilder();
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{1,}", options);
            Regex regex1 = new Regex(@".*(\b\d{1}\.\d{2}\.\d{2})", options);
            Regex regex2 = new Regex(@"(\d{1,2})\.(\d{2})\.(\d{2})", options);
            //Regex regex3 = new Regex(@"(\d{2}\.\d{2}\.\d{1,2})\;(\d{1,2}\:\d{2})", options);
            Regex regex3 = new Regex(@"(\d{2}\.\d{2}\.\d{1,2})\,(\d{1,2}\:\d{2})", options);
            string line1, line2, line3, line4, line5, line6;

            foreach (string line in lines)
            {
                //line1 = line.Replace(' ', ';');
                line1 = line.Replace(' ',',');
                //line1 = regex.Replace(line, ";");
                line1 = regex.Replace(line, ",");
                line2 = regex1.Replace(line1, "$1");
                line3 = regex2.Replace(line2, "$3.$2.$1");
                line4 = regex3.Replace(line3, "$1 $2");
                line5 = line4.Replace(",","','");
                line6 = line5.Replace("N", "С");
                line6 = line6.Replace("NNE", "ССВ");
                line6 = line6.Replace("NE", "СВ");
                line6 = line6.Replace("ENE", "ВСВ");
                line6 = line6.Replace("E", "В");
                line6 = line6.Replace("ESE", "ВЮВ");
                line6 = line6.Replace("SE", "ЮВ");
                line6 = line6.Replace("SSE", "ЮЮВ");
                line6 = line6.Replace("S", "Ю");
                line6 = line6.Replace("SSW", "ЮЮЗ");
                line6 = line6.Replace("SW", "ЮЗ");
                line6 = line6.Replace("WSW", "ЗЮЗ");
                line6 = line6.Replace("W", "З");
                line6 = line6.Replace("WNW", "ЗСЗ");
                line6 = line6.Replace("NW", "СЗ");
                line6 = line6.Replace("NNW", "ССЗ");
                sb.AppendLine(line6);
            }

            File.WriteAllText(OutputhFilePath, sb.ToString());
            //INSERT//
            var last_line = File.ReadAllLines(OutputhFilePath).Reverse();
            File.WriteAllLines(OutputhFilePath, last_line.Take(1).ToArray());
            var last_line1 = File.ReadAllLines(OutputhFilePath).First().ToString();
            /*string last_line2 = last_line1[0];*/
            
            ///////////////////////MYSQL///////////////////////
            var serializer = new JavaScriptSerializer();
            var json = File.ReadAllText(@"C:\Users\Admin\Desktop\weatherextract\DB.json");
            //var json = File.ReadAllText(@"C:\Users\user\Desktop\DB.json"); //localhost
            var json_data = serializer.Deserialize<Jsondata[]>(json);
            string connStr = $"server={json_data[0].Server};user={json_data[0].User};database={json_data[0].Database};password={json_data[0].Password};Allow User Variables = True";
            //string connStr = "server=localhost;user=root;database=weatherlink;password=root;Allow User Variables = True";
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();
            _ = new MySqlCommand();
            MySqlCommand cmd = conn.CreateCommand();

            //string query = $"SET @OLDMAXID = IFNULL((SELECT MAX(id) FROM weatherlink), 0); LOAD DATA INFILE '{OutputhFilePath}' IGNORE INTO TABLE weatherlink FIELDS TERMINATED BY ';' LINES TERMINATED BY '\r\n' IGNORE 3 LINES; DELETE FROM weatherlink WHERE date_time < (SELECT * FROM(SELECT MAX(date_time) AS MaxDatetime FROM weatherlink) AS t) and id > @OLDMAXID";
            string query = $"INSERT IGNORE INTO weatherlink VALUES ('{last_line1} '); DELETE weatherlink FROM weatherlink INNER JOIN (SELECT MAX(id) as lastId, date_time FROM weatherlink GROUP BY date_time HAVING COUNT(*) > 1) duplic on duplic.date_time = weatherlink.date_time WHERE weatherlink.id < duplic.lastId;";
            
            cmd.CommandText = query;
            cmd.ExecuteNonQuery();

            conn.Close();
        }
        private static void TimerCallback(object sender, ElapsedEventArgs e)
        {
            Console.WriteLine(DateTime.Now);
            GC.Collect();
        }
    }
}