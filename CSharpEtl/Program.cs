using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System;
using System.Collections.Generic;
using System.Linq;
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
            Stopwatch mainStopWatch = new Stopwatch();
            Stopwatch stopwatch = new Stopwatch();
            int allDataCount = 0;
            orclConnStr = File.ReadAllLines("oracleConnStr.txt")[0];
            mainStopWatch.Start();
            Console.WriteLine("BEGIN TRANSFER");
            OracleConnection oracleConnection = new OracleConnection(orclConnStr);
            try
            {
                oracleConnection.Open();
                using (var adapter = new Subscription_typeTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_Id = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.ID).ToArray()
                    };
                    OracleParameter p_Prize = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Prize).ToArray()
                    };
                    OracleParameter p_Desc = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.Special).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.SUBSCRIPTION_PLAN (ID, COST, DESCRIPTION) VALUES (:1,:2,:3)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_Id);
                    cmd.Parameters.Add(p_Prize);
                    cmd.Parameters.Add(p_Desc);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("SUBSCRIPTION_PLAN DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new CountryTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_Name = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.Name).ToArray()
                    };
                    OracleParameter p_Population = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Population).ToArray()
                    };
                    OracleParameter p_Lang = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.Main_language).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.COUNTRY (NAME, POPULATION, LANGUAGE) VALUES (:1, :2, :3)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_Name);
                    cmd.Parameters.Add(p_Population);
                    cmd.Parameters.Add(p_Lang);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("COUNTRY DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new REMOTE_PLACETableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_ID = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS"+s.ID).ToArray()
                    };
                    OracleParameter p_Name = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.NAME).ToArray()
                    };
                    OracleParameter p_Population = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.POPULATION).ToArray()
                    };
                    OracleParameter p_xCord = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.X_COORDINATE).ToArray()
                    };
                    OracleParameter p_yCord = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Y_COORDINATE).ToArray()
                    };
                    OracleParameter p_CountryName = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.COUNTRYNAME).ToArray()
                    };
                    OracleParameter p_TransmitterId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.TRANSMITER_ID).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.PLACE(ID, CITY, POPULATION, X_COORD, Y_COORD, COUNTRYNAME, TRANSMITERID) VALUES(:1, :2, :3, :4, :5, :6, :7)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_ID);
                    cmd.Parameters.Add(p_Name);
                    cmd.Parameters.Add(p_Population);
                    cmd.Parameters.Add(p_xCord);
                    cmd.Parameters.Add(p_yCord);
                    cmd.Parameters.Add(p_CountryName);
                    cmd.Parameters.Add(p_TransmitterId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("PLACE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new CallTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_ID = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS"+s.ID).ToArray()
                    };
                    OracleParameter p_DateStart = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => (OracleTimeStamp)s.Date_Started).ToArray()
                    };
                    OracleParameter p_DateEnd = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => (OracleTimeStamp)s.Date_Started.AddMinutes(s.Duration)).ToArray()
                    };
                    OracleParameter p_Type = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => {
                            if (s.Is_Conference && s.Is_Video) return "VIDEO_CONFERENCE";
                            else if (s.Is_Conference && !s.Is_Video) return "CONFERENCE";
                            else if (!s.Is_Conference && s.Is_Video) return "VIDEO";
                            else return "REGULAR";
                        }).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.CALL (ID, START_TIME, END_TIME, TYPE) VALUES (:1, :2, :3, :4)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_ID);
                    cmd.Parameters.Add(p_DateStart);
                    cmd.Parameters.Add(p_DateEnd);
                    cmd.Parameters.Add(p_Type);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("CALL DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new AllMessagesTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_ID = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.ID).ToArray()
                    };
                    OracleParameter p_Length = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.LENGTH).ToArray()
                    };
                    OracleParameter p_DateSent = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => (OracleTimeStamp)s.DATE_SENT).ToArray()
                    };
                    OracleParameter p_DataAmount = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.DATA_AMOUNT).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.MESSAGE (ID, LENGTH, DATE_SENT, DATA_AMOUNT) VALUES (:1, :2, :3, :4)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_ID);
                    cmd.Parameters.Add(p_Length);
                    cmd.Parameters.Add(p_DateSent);
                    cmd.Parameters.Add(p_DataAmount);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("MESSAGE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                stopwatch.Start();
                List<string> RemotePlaceData = new List<string>();
                OracleCommand readCmd = new OracleCommand()
                {
                    Connection = oracleConnection,
                    CommandText = "SELECT ID FROM ii738.PLACE ORDER BY TRANSMITERID",
                    CommandType = System.Data.CommandType.Text
                };
                using (var rdr = readCmd.ExecuteReader())
                    while (rdr.Read())
                        RemotePlaceData.Add(rdr.GetValue(0).ToString());
                stopwatch.Stop();
                Console.WriteLine("FETCHED REMOTE PLACE DATA IN {0}",stopwatch.Elapsed);
                stopwatch.Reset();

                using (var adapter = new Transmiter_CallTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_CallId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.CallID).ToArray()
                    };
                    OracleParameter p_PlaceId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePlaceData[(int)s.TransmiterID]).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.CALL_PLACE (CALLID, PLACEID) VALUES (:1, :2)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_CallId);
                    cmd.Parameters.Add(p_PlaceId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("CALL_PLACE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new Transmiter_MessageTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_MessageId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.MessageID).ToArray()
                    };
                    OracleParameter p_PlaceId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePlaceData[(int)s.TransmiterID]).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.MESSAGE_PLACE (MESSAGEID, PLACEID) VALUES (:1, :2)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_MessageId);
                    cmd.Parameters.Add(p_PlaceId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("MESSAGE_PLACE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                //RESTART CONNECTION
                oracleConnection.Dispose();
                oracleConnection = new OracleConnection(orclConnStr);
                oracleConnection.Open();
                Console.WriteLine("RESETING CONNECTION");

                using (var adapter = new ALL_SUBSTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_Id = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.ID).ToArray()
                    };
                    OracleParameter p_Name = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.NAME).ToArray()
                    };
                    OracleParameter p_Surname = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        IsNullable = true,
                        Value = data.Select(s => s.SURNAME).ToArray()
                    };
                    OracleParameter p_Sex = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.SEX).ToArray()
                    };
                    OracleParameter p_StartDate = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => s.START_DATE).ToArray()
                    };
                    OracleParameter p_EndDate = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => s.END_DATE).ToArray()
                    };
                    OracleParameter p_SubscriptionPlanId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.SUBSCRIPTION_PLANID).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.ACCOUNT (ID, NAME, SURNAME, SEX, START_DATE, END_DATE, SUBSCRIPTION_PLANID) VALUES (:1, :2, :3, :4, :5, :6, :7)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_Id);
                    cmd.Parameters.Add(p_Name);
                    cmd.Parameters.Add(p_Surname);
                    cmd.Parameters.Add(p_Sex);
                    cmd.Parameters.Add(p_StartDate);
                    cmd.Parameters.Add(p_EndDate);
                    cmd.Parameters.Add(p_SubscriptionPlanId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("ACCOUNT DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new AllPhoneNumsTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_Id = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.ID).ToArray()
                    };
                    OracleParameter p_PhoneNumber = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Phone_Number).ToArray()
                    };
                    OracleParameter p_isActive = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Is_Active).ToArray()
                    };
                    OracleParameter p_AccountId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => s.Sub_Id).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.PHONE_NUMBER (ID, PH_NUMBER, IS_ACTIVE, ACCOUNTID) VALUES (:1, :2, :3, :4)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_Id);
                    cmd.Parameters.Add(p_PhoneNumber);
                    cmd.Parameters.Add(p_isActive);
                    cmd.Parameters.Add(p_AccountId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("PHONE_NUMBER DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                stopwatch.Start();
                Dictionary<string,string> RemotePhoneNumberData = new Dictionary<string, string>();
                OracleCommand readPhoneCmd = new OracleCommand()
                {
                    Connection = oracleConnection,
                    CommandText = "SELECT ID, PH_NUMBER FROM ii738.PHONE_NUMBER",
                    CommandType = System.Data.CommandType.Text
                };
                using (var rdr = readPhoneCmd.ExecuteReader())
                    while (rdr.Read())
                        RemotePhoneNumberData.Add(rdr.GetValue(1).ToString(),rdr.GetValue(0).ToString());
                stopwatch.Stop();
                Console.WriteLine("FETCHED REMOTE PHONE NUMBER DATA IN {0}", stopwatch.Elapsed);
                stopwatch.Reset();

                using (var adapter = new Internet_Packet_UsageTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_Id = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.ID).ToArray()
                    };
                    OracleParameter p_StartDate = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => s.Date_started).ToArray()
                    };
                    OracleParameter p_EndDate = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.TimeStamp,
                        Value = data.Select(s => s.Date_ended).ToArray()
                    };
                    OracleParameter p_Amount = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Int32,
                        Value = data.Select(s => s.Bytes_uploaded+s.Bytes_downloaded).ToArray()
                    };
                    OracleParameter p_PhoneNumberId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePhoneNumberData[s.NumberNumber]).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.DATA_TRANSMISSION (ID, START_DATE, END_DATE, AMOUNT, PHONE_NUMBERID) VALUES (:1, :2, :3, :4, :5)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_Id);
                    cmd.Parameters.Add(p_StartDate);
                    cmd.Parameters.Add(p_EndDate);
                    cmd.Parameters.Add(p_Amount);
                    cmd.Parameters.Add(p_PhoneNumberId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("DATA_TRANSMISSION DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new Trans_Inter_Pac_UsgTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_TransmissionId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.Internet_Packet_UsageID).ToArray()
                    };
                    OracleParameter p_PlaceId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePlaceData[(int)s.TransmiterID]).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.DATA_TRANSMISSION_PLACE (DATA_TRANSMISSIONID, PLACEID) VALUES (:1, :2)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_TransmissionId);
                    cmd.Parameters.Add(p_PlaceId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("DATA_TRANSMISSION_PLACE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new Number_CallTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_PhoneNumberId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePhoneNumberData[s.NumberNumber]).ToArray()
                    };
                    OracleParameter p_CallId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS"+s.CallID).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.PHONE_NUMBER_CALL (PHONE_NUMBERID, CALLID) VALUES (:1, :2)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_PhoneNumberId);
                    cmd.Parameters.Add(p_CallId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("PHONE_NUMBER_CALL DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }

                using (var adapter = new Number_MessageTableAdapter())
                {
                    stopwatch.Start();
                    var data = adapter.GetData();
                    OracleParameter p_PhoneNumberId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => RemotePhoneNumberData[s.NumberNumber]).ToArray()
                    };
                    OracleParameter p_MessageId = new OracleParameter()
                    {
                        OracleDbType = OracleDbType.Varchar2,
                        Value = data.Select(s => "MS" + s.MessageID).ToArray()
                    };
                    OracleCommand cmd = oracleConnection.CreateCommand();
                    cmd.CommandText = "INSERT INTO ii738.PHONE_NUMBER_MESSAGE (PHONE_NUMBERID, MESSAGEID) VALUES (:1, :2)";
                    cmd.ArrayBindCount = data.Count;
                    cmd.Parameters.Add(p_PhoneNumberId);
                    cmd.Parameters.Add(p_MessageId);
                    cmd.ExecuteNonQuery();
                    stopwatch.Stop();
                    allDataCount += data.Count;
                    Console.WriteLine("PHONE_NUMBER_MESSAGE DONE, {0} RECORDS IN {1}", data.Count, stopwatch.Elapsed);
                    stopwatch.Reset();
                }
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
                mainStopWatch.Stop();
                oracleConnection.Dispose();
                Console.WriteLine("DONE!");
                Console.WriteLine("TRANSFERED {0} RECORDS IN {1}", allDataCount, mainStopWatch.Elapsed);
                Console.ReadLine();
            }
        }
    }
}
