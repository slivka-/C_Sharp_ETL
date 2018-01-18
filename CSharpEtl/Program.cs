using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSharpEtl.ZDB_TELCOMDataSetTableAdapters;

namespace CSharpEtl
{
    class Program
    {
        private static readonly string orclConnStr = "";

        static void Main(string[] args)
        {
            
            OracleConnection oracleConnection = new OracleConnection(orclConnStr);
            try
            {
                oracleConnection.Open();
                
                string SUBSCRIPTION_PLAN_INSERT = "INSERT INTO ii738.SUBSCRIPTION_PLAN VALUES ('MS{0}',{1},'{2}')";
                using (var adapter = new Subscription_typeTableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(SUBSCRIPTION_PLAN_INSERT, s.ID, s.Prize, s.Special)).ToList();
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("SUBS_DONE");

                string COUNTRY_INSERT = "INSERT INTO ii738.COUNTRY VALUES ('{0}',{1},'{2}')";
                using (var adapter = new CountryTableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(COUNTRY_INSERT, s.Name, s.Population, s.Main_language)).ToList();
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("COUNTRY_INSERT DONE");

                string PLACE_INSERT = "INSERT INTO ii738.PLACE VALUES('MS{0}','{1}',{2},{3},{4},'{5}',NULL,{6})";
                using (var adapter = new REMOTE_PLACETableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(PLACE_INSERT, s.ID, s.NAME, s.POPULATION,s.X_COORDINATE,s.Y_COORDINATE,s.COUNTRYNAME,s.TRANSMITER_ID)).ToList();
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("PLACE_INSERT DONE");
                
                string CALL_INSERT = "INSERT INTO ii738.CALL VALUES('MS{0}',TO_TIMESTAMP('{1}','DD.MM.YYYY HH24:MI:SS'),TO_TIMESTAMP('{2}','DD.MM.YYYY HH24:MI:SS'),'REGULAR',NULL)";
                using (var adapter = new CallTableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(CALL_INSERT,s.ID,s.Date_Started.ToString("dd.MM.yyyy HH:mm:ss"),s.Date_Started.AddMinutes(s.Duration).ToString("dd.MM.yyyy HH:mm:ss"))).ToList();
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("CALL_INSERT DONE");
                
                string MESSAGE_INSERT = "INSERT INTO ii738.MESSAGE VALUES('MS{0}',{1},TO_TIMESTAMP('{2}','DD.MM.YYYY HH24:MI:SS'),NULL,NULL)";
                using (var adapter = new MessageTableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(MESSAGE_INSERT, s.ID, s.Msg_Length, s.Date_Sent.ToString("dd.MM.yyyy HH:mm:ss"))).ToList();
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("MESSAGE_INSERT DONE");
                
                string ACCOUNT_INSERT = "INSERT INTO ii738.ACCOUNT VALUES('MS{0}','{1}',NULL,'{2}',NULL,'{3}',NULL,TO_TIMESTAMP('{4}','DD.MM.YYYY HH24:MI:SS'),TO_TIMESTAMP('{5}','DD.MM.YYYY HH24:MI:SS'),NULL,'{6}')";
                using (var adapter = new ALL_SUBSTableAdapter())
                {
                    var queries = adapter.GetData().Select(s => string.Format(ACCOUNT_INSERT, s.ID, s.NAME, s.SURNAME, s.SEX, s.START_DATE, s.END_DATE, s.SUBSCRIPTION_PLANID)).ToList();
                    int c = 0;
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                        Console.SetCursorPosition(1, 0);
                        Console.WriteLine(c+" / "+queries.Count);
                    }
                }
                Console.WriteLine("ACCOUNT_INSERT DONE");
                
                string PHONE_NUMBER_INSERT = "INSERT INTO ii738.PHONE_NUMBER VALUES('MS{0}', {1}, {2},'{3}')";
                using (var adapter = new ViewTableAdapter())
                {
                    int tempC = 0;
                    var queries = adapter.GetData().Select(s => string.Format(PHONE_NUMBER_INSERT, tempC++, s.Phone_Number, (s.Is_Active)?1:0, s.SUB_ID)).ToList();
                    int c = 0;
                    foreach (var q in queries)
                    {
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                        Console.SetCursorPosition(1, 0);
                        Console.WriteLine(c + " / " + queries.Count);
                    }
                }
                Console.WriteLine("PHONE_NUMBER_INSERT DONE");
            }
            catch (OracleException ex)
            {
                Console.WriteLine("!!!!ORCL EX!!!!");
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                oracleConnection.Dispose();
                Console.WriteLine("DONE!");
                Console.ReadLine();
            }
        }
    }
}
