using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Text.RegularExpressions;
using System.Web.Script.Serialization;

namespace ConsoleApp4
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
        private static DateTime _NextCallTime;
        private static int MinSteps = 1;

        private static void Main(string[] args)
        {
            if (args is null)
            {
                throw new ArgumentNullException(nameof(args));
            }
            // set start time
            Console.Write("Введите начальное время: ");
            string date_time = Console.ReadLine();
            _NextCallTime = DateTime.Parse($"{DateTime.Now.ToString("yyyy/MM/dd", System.Globalization.CultureInfo.InvariantCulture)} {date_time}");
            System.Timers.Timer timer = new System.Timers.Timer()
            { Interval = 31000, Enabled = true };
            timer.Elapsed += Timer_Handler;
            Console.ReadLine();
        }
        // this timer checks every 31 seconds
        static void Timer_Handler(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (DateTime.Now.Date == _NextCallTime.Date
               && DateTime.Now.Hour == _NextCallTime.Hour
               && DateTime.Now.Minute == _NextCallTime.Minute)
            {
                _NextCallTime = _NextCallTime.AddMinutes(MinSteps);
                Console.WriteLine(DateTime.Now);
                Load_to_DB();
            }

        }
        // This method is called every 30 mins at specific periods
        static void Load_to_DB()
        {
            string FilePath = @"\\OBMEN\WeatherLink\Garmony\download.txt";
            string OutputhFilePath = @"C:/users/user/desktop/download12.txt";
            /*string OutputhFilePath = "C:/users/user/desktop/openserver/userdata/php_upload/download11.txt";*/
            /*string FilePath = @"D:\Desktop\download1.txt";//home
            string OutputhFilePath = @"D:/Desktop/OpenServer/userdata/php_upload/download11.txt";//home*/

            _ = new List<string>();
            List<string> lines = File.ReadAllLines(FilePath).ToList();

            var sb = new StringBuilder();
            RegexOptions options = RegexOptions.None;
            Regex regex = new Regex("[ ]{1,}", options);
            Regex regex1 = new Regex(@".*(\b\d{1}\.\d{2}\.\d{2})", options);
            Regex regex2 = new Regex(@"(\d{1,2})\.(\d{2})\.(\d{2})", options);
            Regex regex3 = new Regex(@"(\d{2}\.\d{2}\.\d{1,2})\;(\d{1,2}\:\d{2})", options);
            string line1, line2, line3, line4;

            foreach (string line in lines)
            {
                line1 = line.Replace(' ', ';');
                line1 = regex.Replace(line, ";");
                line2 = regex1.Replace(line1, "$1");
                line3 = regex2.Replace(line2, "$3.$2.$1");
                line4 = regex3.Replace(line3, "$1 $2");
                sb.AppendLine(line4);
            }

            File.WriteAllText(OutputhFilePath, sb.ToString());

            ///////////////////////MYSQL///////////////////////
            var serializer = new JavaScriptSerializer();
            var json = File.ReadAllText(@"C:\Users\user\Desktop\DB.json");
            var json_data = serializer.Deserialize<Jsondata[]>(json);
            string connStr = $"server={json_data[0].Server};user={json_data[0].User};database={json_data[0].Database};password={json_data[0].Password};Allow User Variables = True";
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();
            _ = new MySqlCommand();
            MySqlCommand cmd = conn.CreateCommand();

            string query = $"SET @OLDMAXID = IFNULL((SELECT MAX(id) FROM weatherlink), 0); LOAD DATA INFILE '{OutputhFilePath}' IGNORE INTO TABLE weatherlink FIELDS TERMINATED BY ';' LINES TERMINATED BY '\r\n' IGNORE 3 LINES; DELETE FROM weatherlink WHERE date_time < (SELECT * FROM(SELECT MAX(date_time) AS MaxDatetime FROM weatherlink) AS t) and id > @OLDMAXID";

            cmd.CommandText = query;
            cmd.ExecuteNonQuery();

            conn.Close();
        }
    }
}
