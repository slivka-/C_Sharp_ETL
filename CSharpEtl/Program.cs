using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSharpEtl.ZDB_TELCOMDataSetTableAdapters;
using System.IO;
using System.Diagnostics;

namespace CSharpEtl
{
    class Program
    {
        private static string orclConnStr;

        static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            orclConnStr = File.ReadAllLines("oracleConnStr.txt")[0];
            Console.CursorTop = 1;

            OracleConnection oracleConnection = new OracleConnection(orclConnStr);
            try
            {
                oracleConnection.Open();
                stopwatch.Start();
                string SUBSCRIPTION_PLAN_INSERT = "INSERT INTO ii738.SUBSCRIPTION_PLAN (ID, COST, DESCRIPTION) VALUES (:1,:2,:3)";
                using (var adapter = new Subscription_typeTableAdapter())
                {
                    var l = adapter.GetData().ToArray().Length;

                    OracleParameter p_Id = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = adapter.GetData().Select(s => "MS" + s.ID).ToArray()
                    };

                    OracleParameter p_Prize = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = adapter.GetData().Select(s => s.Prize).ToArray()
                    };

                    OracleParameter p_Desc = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = adapter.GetData().Select(s => s.Special).ToArray()
                    };

                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = SUBSCRIPTION_PLAN_INSERT;
                    cmd.ArrayBindCount = l;
                    cmd.Parameters.Add(p_Id);
                    cmd.Parameters.Add(p_Prize);
                    cmd.Parameters.Add(p_Desc);
                    cmd.ExecuteNonQuery();

                    /*
                        OracleCommand cmd = new OracleCommand
                        {
                            Connection = oracleConnection,
                            CommandText = q,
                            CommandType = System.Data.CommandType.Text
                        };
                        cmd.ExecuteNonQuery();
                        Console.SetCursorPosition(1, Console.CursorTop-1);
                        Console.WriteLine(c++ + " / " + queries.Count);*/

                }
                stopwatch.Stop();
                Console.WriteLine("SUBSCRIPTION_PLAN DONE IN "+ stopwatch.Elapsed);
                Console.WriteLine();
                stopwatch.Reset();

                stopwatch.Start();
                string COUNTRY_INSERT = "INSERT INTO ii738.COUNTRY VALUES ('{0}',{1},'{2}')";
                using (var adapter = new CountryTableAdapter())
                {
                    int c = 1;
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
                        Console.SetCursorPosition(1, Console.CursorTop-1);
                        Console.WriteLine(c++ + " / " + queries.Count);
                    }
                }
                stopwatch.Stop();
                Console.WriteLine("COUNTRY_INSERT DONE IN " + stopwatch.Elapsed);
                Console.WriteLine();
                stopwatch.Reset();

                /*
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
                */
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
