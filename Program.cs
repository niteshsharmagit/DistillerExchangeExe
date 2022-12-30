using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;

namespace DistillerExchangeExe
{
    class Program
    {
        public static string FilePath = string.Empty;
        static void Main(string[] args)
        {
            try
            {
            SqlConnection con = new SqlConnection("server=3.20.32.190; database=DistillerExchange; user=sa; password=lms123; Persist Security Info=False; Connect Timeout=25000; MultipleActiveResultSets=True;");
            con.Open();

                 FilePath = @"C:\DotNet\DXLab\DistillateExchangeExe";
                //FilePath = @"C:\DxLab - EXE\DistillerExchangeExe";
                //FilePath = @"C:\Users\Nitesh Sharma\source\repos\DistillerExchangeExe";
                //FilePath = @"D:\LMS\DistillerExchangeExe";

                if (!File.Exists(FilePath + "\\" + "log.txt"))
                File.Create(FilePath + "\\" + "log.txt");


                using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                      AppendLog("Data is updated or inserted for BidId " + 1, w);

                    SqlCommand cmd = new SqlCommand("select * from tblOilStockForSell Where Active = 1", con);

            using (SqlDataReader oReader = cmd.ExecuteReader())
            {
                if (oReader.HasRows == true)
                {
                    Program obj = new Program();

                    while (oReader.Read())
                    {
                        DateTime CreatedDate = (DateTime)oReader["CreatedDate"];
                        int GoodForId = (int)oReader["GoodForId"];
                        int ForSellId = (int)oReader["ForSellId"];
                        int UserId = (int)oReader["UserId"];
                        string ProductTypeName =  obj.GetProductTypeName((int)oReader["ProductTypeId"], con)  ;

                        string BidDetailsQuery = "select * from tblBidDetails Where ForSellId = " + ForSellId;
                        SqlCommand BidDetailscmd = new SqlCommand(BidDetailsQuery, con);
                        SqlDataReader BidDetailsReader = BidDetailscmd.ExecuteReader();

                        if (BidDetailsReader.HasRows == false)
                        {
                            int GoodForHours = obj.GetGoodForHours(GoodForId, con);
                            SqlDataReader DateDiffReader = obj.GetDateDifference(con, CreatedDate);
                            while (DateDiffReader.Read())
                            {
                                if ((int)DateDiffReader["DateDiff"] > GoodForHours)
                                {
                                    // Active false of ForSell and Qty added to Wallet
                                    obj.UpdateForSellAndInsetWalletData(ForSellId, con, oReader);
                                    obj.AddNotification(UserId, "Your product " + ProductTypeName + " has expired.", UserId, con);
                                }
                            }
                        }
                        else if (BidDetailsReader.HasRows == true)
                        {
                            bool IsUpdateForSellAndInsetWalletDataExecute = false;

                            while (BidDetailsReader.Read())
                            {
                                string SellQuery = "select * from tblSell Where BidId = " + (int)BidDetailsReader["BidId"];
                                SqlCommand Sellcmd = new SqlCommand(SellQuery, con);
                                SqlDataReader SellReader = Sellcmd.ExecuteReader();

                                if (SellReader.HasRows == false && (bool)BidDetailsReader["Active"] == true)
                                {                                   
                                    int GoodForHours = obj.GetGoodForHours(GoodForId, con);
                                    SqlDataReader DateDiffReader = obj.GetDateDifference(con, CreatedDate);
                                    while (DateDiffReader.Read())
                                    {
                                        if ((int)DateDiffReader["DateDiff"] > GoodForHours)
                                        {
                                            // Bid Cancel
                                            obj.UpdateBidDetailsData((int)BidDetailsReader["BidId"], con);
                                            obj.AddNotification(UserId, "Your bid has been cancelled due to expiry of the product " + ProductTypeName , (int)BidDetailsReader["UserId"], con);
                                                if (IsUpdateForSellAndInsetWalletDataExecute == false)
                                                {
                                                    IsUpdateForSellAndInsetWalletDataExecute = true;
                                                    obj.UpdateForSellAndInsetWalletData(ForSellId, con, oReader);
                                                    obj.AddNotification(UserId, "Your product " + ProductTypeName + " has expired.", UserId, con);
                                                }                                            
                                        }
                                    }
                                }

                                    // If bid is selled then do nothing.

                                    else if (SellReader.HasRows == true && (bool)BidDetailsReader["Active"] == false)
                                    {
                                        int GoodForHours = obj.GetGoodForHours(GoodForId, con);
                                        SqlDataReader DateDiffReader = obj.GetDateDifference(con, CreatedDate);
                                        while (DateDiffReader.Read())
                                        {
                                            if ((int)DateDiffReader["DateDiff"] > GoodForHours)
                                            {
                                                if (IsUpdateForSellAndInsetWalletDataExecute == false)
                                                {
                                                    IsUpdateForSellAndInsetWalletDataExecute = true;
                                                    obj.UpdateForSellAndInsetWalletData(ForSellId, con, oReader);
                                                    obj.AddNotification(UserId, "Your product " + ProductTypeName + " has expired.", UserId, con);
                                                }
                                            }
                                        }
                                    }
                                }



                                //if (IsAnyBidSelled == false) //Active false of ForSell and Qty added to Wallet.
                                //{
                                //        int GoodForHours = obj.GetGoodForHours(GoodForId, con);
                                //        SqlDataReader DateDiffReader = obj.GetDateDifference(con, CreatedDate);
                                //        while (DateDiffReader.Read())
                                //        {
                                //            if ((int)DateDiffReader["DateDiff"] > GoodForHours)
                                //            {
                                //                obj.UpdateForSellAndInsetWalletData(ForSellId, con, oReader);
                                //            }
                                //        }                                  
                                //}
                            }
                    }
                }
                con.Close();
            }
            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message, w);
            }

        }

        public string GetProductTypeName(int ProductTypeId, SqlConnection con)
        {
            string GoodforText = String.Empty;
            try
            {
                string ProductTypeQuery = "select * from tblProductMaster Where ProductID = " + ProductTypeId;
                SqlCommand ProductTypecmd = new SqlCommand(ProductTypeQuery, con);
                SqlDataReader ProductTypeQueryReader = ProductTypecmd.ExecuteReader();
                while (ProductTypeQueryReader.Read())
                {
                   GoodforText = ProductTypeQueryReader["ProductName"].ToString();
                }
            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message, w);
            }
            return GoodforText;
        }

        public int GetGoodForHours(int GoodForId, SqlConnection con)
        {
            int GoodForHours = 0;
            try
            {
                string GoodForQuery = "select * from tblGoodFor Where GoodForId = " + GoodForId;
                SqlCommand GoodForcmd = new SqlCommand(GoodForQuery, con);
                SqlDataReader GoodForReader = GoodForcmd.ExecuteReader();
                while (GoodForReader.Read())
                {
                    string GoodforText = new String(GoodForReader["GoodForText"].ToString().Where(Char.IsDigit).ToArray());
                    GoodForHours = Int32.Parse(GoodforText) * 24 * 60;
                }
            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message, w);
            }
            return GoodForHours;
        }
        public SqlDataReader GetDateDifference(SqlConnection con, DateTime CreatedDate)
        {
            SqlDataReader DateDiffReader = null;
            try
            {
                string DateDiffQuery = "SELECT DATEDIFF(MINUTE, '" + CreatedDate.ToString("yyyy-MM-dd HH:mm:ss") + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "') AS DateDiff";
                SqlCommand DateDiffcmd = new SqlCommand(DateDiffQuery, con);
                DateDiffReader = DateDiffcmd.ExecuteReader();
            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message, w);
            }
            return DateDiffReader;
        }
        public void UpdateForSellAndInsetWalletData(int ForSellId, SqlConnection con, SqlDataReader oReader)
        {
            try
            {
                string OilStockForSellUpdateQuery = "Update tblOilStockForSell Set Active = 0 Where ForSellId = " + ForSellId;
                SqlCommand OilStockForSellUpdatecmd = new SqlCommand(OilStockForSellUpdateQuery, con);
                OilStockForSellUpdatecmd.ExecuteNonQuery();

                string datetime = string.Format("convert(datetime2, '{0:s}')", DateTime.Now);

                Decimal RemainingQuantity = (Decimal)oReader["Quantity"] - (Decimal)oReader["SoldQuantity"];

                string OilWalletInsertQuery = "Insert into tblOilWallet (OilId,ProductTypeId,Quantity,UserId,SellerId,Active,CreatedDate,ModifiedDate,Decision,TrxType) VALUES ("
                        + (int)oReader["OilId"] + "," + (int)oReader["ProductTypeId"] + "," + RemainingQuantity + "," + (int)oReader["UserId"] + "," + (int)oReader["UserId"]
                        + "," + 1 + "," + datetime + "," + datetime + "," + 1 + "," + 3 + ")";
                SqlCommand OilWalletInsertcmd = new SqlCommand(OilWalletInsertQuery, con);
                OilWalletInsertcmd.ExecuteNonQuery();

                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog("Data is updated or inserted for ForSellId " + ForSellId, w);               
            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message + "ForSellId " + ForSellId, w);
            }
        }
        public void UpdateBidDetailsData(int BidId, SqlConnection con)
        {
            try
            {
                string BidDetailsUpdateQuery = "Update tblBidDetails Set Active = 0 Where BidId = " + BidId;
                SqlCommand BidDetailsUpdatecmd = new SqlCommand(BidDetailsUpdateQuery, con);
                BidDetailsUpdatecmd.ExecuteNonQuery();

                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog("Data is updated or inserted for BidId " + BidId, w);

            }
            catch (Exception e)
            {
                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog(e.Message + "BidId " + BidId, w);
            }
        }

        public void AddNotification(int FromUserId, string Description, int ToUserId, SqlConnection con)
        {
            try
            {
                string datetime = string.Format("convert(datetime2, '{0:s}')", DateTime.Now);

                string AddNotificationQuery = "Insert into tblNotification (Active,CreatedDate,ModifiedDate,FromUserId,IsSeen,ToUserId,NotificationDesc) VALUES ("
                        + 1 + "," + datetime + "," + datetime + "," + FromUserId + "," + 0 + "," + ToUserId + "," + "'" + Description + "'" + ")";
                SqlCommand AddNotificationInsertcmd = new SqlCommand(AddNotificationQuery, con);
                AddNotificationInsertcmd.ExecuteNonQuery();

                //using (StreamWriter w = File.AppendText(FilePath + "\\" + "log.txt"))
                //    AppendLog("Data is inserted for ToUserId " + ToUserId + "and NotificationDesc = " + Description, w);
            }

            catch (Exception ex)
            {
                throw;
            }
        }

        private static void AppendLog(string logMessage, TextWriter txtWriter)
        {
            try
            {
                txtWriter.Write("\r\nLog Entry : ");
                txtWriter.WriteLine("{0} {1}", DateTime.Now.ToLongTimeString(), DateTime.Now.ToLongDateString());
                txtWriter.WriteLine(logMessage);
                txtWriter.WriteLine("---------------------------------------------------------------------");
            }
            catch (Exception ex)
            {
            }
        }

    }
}
