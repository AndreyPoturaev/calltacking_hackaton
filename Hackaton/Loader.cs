using BAMCIS.PrestoClient;
using BAMCIS.PrestoClient.Interfaces;
using BAMCIS.PrestoClient.Model.Statement;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace Hackaton
{
    public class Loader
    {
        public class CallsInfo
        {
            public string Text { get; set; }
            public int CalltrackingId { get; set; }

            public int RazmetkaId { get; set; }
            public int CalculatedId { get; set; }

            public double Similarity { get; set; }

            public string Error { get; set; }

            public IList<AnnouncementInfo> Announcements { get; set; } = new List<AnnouncementInfo>();
        }

        public class AnnouncementInfo
        {
            public int Id { get; set; }
            public string Street { get; set; }

            public string House { get; set; }

            public int PhoneViewCount { get; set; }

            public double Similarity { get; set; }

            public IList<FeatureInfo> Features { get; set; } = new List<FeatureInfo>();
        }

        public class FeatureInfo
        {
            public string Name { get; set; }

            public double Weight { get; set; }

            public string Data { get; set; }


            public static string ONE_ANNOUNEMENT = "oneAnnouncement";
            public static string HOUSE = "houseNumber";
            public static string LEVENSTAIN = "levenstain";
            public static string JARO = "jaroWinkler";
            public static string COSINE = "cosine";
            public static string PHONE_VIEW = "phoneView";
            public static string ONE_PHONE_VIEW = "onePhoneView";
        }

        private class CsvUserDetailsMapping : CsvMapping<CallsInfo>
        {
            public CsvUserDetailsMapping()
                : base()
            {
                MapProperty(0, x => x.CalltrackingId);
                if (GlobalSettings.CompareMode)
                    MapProperty(1, x => x.RazmetkaId);
            }
        }


        public Loader(ILogger<Loader> logger)
        {
            _logger = logger;
        }


        private readonly ILogger _logger;

        private readonly PrestoClientSessionConfig Config = new PrestoClientSessionConfig("hive", "apoturaev")
        {
            Host = "192.168.177.239",
            Port = 8080
        };        


        public Task<CallsInfo[]> LoadDataAsync()
        {
            var fileName = GlobalSettings.CompareMode ? @"C:\Users\a.poturaev\Desktop\hackaton\rasmetka_2.csv" : @"C:\Users\a.poturaev\Desktop\hackaton\calls_test.csv";
            _logger.LogInformation("Загрузка звонков из csv");

            var csvParserOptions = GlobalSettings.UseCommaDelimeter ? new CsvParserOptions(true, ',') : new CsvParserOptions(true, '|');
            var csvMapper = new CsvUserDetailsMapping();
            var csvParser = new CsvParser<CallsInfo>(csvParserOptions, csvMapper);                        
            var result = csvParser
                         .ReadFromFile(fileName, Encoding.UTF8);            

            var info = result.Select(x =>
            {
                if (x.IsValid)
                    return x.Result;
                else
                {
                    if (x.Error.ColumnIndex == 0)
                    {
                        _logger.LogError($"Parsing error! rowIndex {x.RowIndex}, columnIndex {x.Error.ColumnIndex}, error {x.Error.Value}");
                        return null;
                    }
                    else
                    {
                        _logger.LogTrace($"Parsing error! rowIndex {x.RowIndex}, columnIndex {x.Error.ColumnIndex}, error {x.Error.Value}");
                        return x.Result;
                    }
                }
            }).Where(x => x != null).ToArray();

            _logger.LogInformation("Закончили");

            return Task.FromResult(info);
        }

        public async Task LoadTextsAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Загрузка текстов звонков из Presto");

            IPrestoClient client = new PrestodbClient(Config);

            var query = @"
select * from apoturaev.hackaton_dataset2_stage1 where calltrackingid in (ID)
";

            var batch = 500;
            var batches = Math.Ceiling((decimal)calls.Length / batch);

            for (int i=0; i<batches; i++)
            {
                var loadCount = Math.Min(batch, calls.Length - i * batch);

                var ids = calls.Skip(i * batch).Take(loadCount).Select(x => x.CalltrackingId);

                var stringIds = String.Join(',', ids.Select(x => x.ToString()).ToArray());

                var parsedQuery = query.Replace("ID", stringIds);

                var request = new ExecuteQueryV1Request(parsedQuery);

                var response = await client.ExecuteQueryV1(request);

                foreach (var data in response.Data)
                {
                    var call = calls.First(x => x.CalltrackingId == Int32.Parse(data[1].ToString()));
                    call.Text = data[0].ToString();
                }

                if (response.LastError != null)
                    _logger.LogError("Error while loading texts");

                _logger.LogInformation($"Batch {i} loaded");
            }

            _logger.LogInformation("Закончили");
        }

        public async Task LoadAnnouncementsStreetsAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Загрузка данных по улицам объявлений из Presto");

            IPrestoClient client = new PrestodbClient(Config);

            var query = @"
select * from apoturaev.hackaton_dataset2_stage2 where calltrackingid in (ID)
";

            var batch = 500;
            var batches = Math.Ceiling((decimal)calls.Length / batch);

            for (int i = 0; i < batches; i++)
            {
                var loadCount = Math.Min(batch, calls.Length - i * batch);

                var ids = calls.Skip(i * batch).Take(loadCount).Select(x => x.CalltrackingId);

                var stringIds = String.Join(',', ids.Select(x => x.ToString()).ToArray());

                var parsedQuery = query.Replace("ID", stringIds);

                var request = new ExecuteQueryV1Request(parsedQuery);

                var response = await client.ExecuteQueryV1(request);

                foreach (var data in response.Data)
                {
                    var call = calls.First(x => x.CalltrackingId == Int32.Parse(data[2].ToString()));

                    var announcementId = Int32.Parse(data[1].ToString());

                    var announcement = call.Announcements.FirstOrDefault(x => x.Id == announcementId);

                    if (announcement == default)
                    {
                        announcement = new AnnouncementInfo
                        {
                            Id = announcementId
                        };

                        call.Announcements.Add(announcement);
                    }

                    announcement.Street = data[0].ToString();                   
                }

                if (response.LastError != null)
                    _logger.LogError("Error while loading texts");

                _logger.LogInformation($"Batch {i} loaded");
            }

            _logger.LogInformation("Закончили");
        }

        public async Task LoadAnnouncementsHousesAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Загрузка данных по домам объявлений из Presto");

            IPrestoClient client = new PrestodbClient(Config);

            var query = @"
select * from apoturaev.hackaton_dataset2_stage8 where calltrackingid in (ID)
";

            var batch = 500;
            var batches = Math.Ceiling((decimal)calls.Length / batch);

            for (int i = 0; i < batches; i++)
            {
                var loadCount = Math.Min(batch, calls.Length - i * batch);

                var ids = calls.Skip(i * batch).Take(loadCount).Select(x => x.CalltrackingId);

                var stringIds = String.Join(',', ids.Select(x => x.ToString()).ToArray());

                var parsedQuery = query.Replace("ID", stringIds);

                var request = new ExecuteQueryV1Request(parsedQuery);

                var response = await client.ExecuteQueryV1(request);

                foreach (var data in response.Data)
                {
                    var call = calls.First(x => x.CalltrackingId == Int32.Parse(data[1].ToString()));

                    var announcementId = Int32.Parse(data[0].ToString());

                    var announcement = call.Announcements.FirstOrDefault(x => x.Id == announcementId);

                    if (announcement == default)
                    {
                        announcement = new AnnouncementInfo
                        {
                            Id = announcementId
                        };

                        call.Announcements.Add(announcement);
                    }

                    announcement.House = data[3].ToString();
                }

                if (response.LastError != null)
                    _logger.LogError("Error while loading texts");

                _logger.LogInformation($"Batch {i} loaded");
            }

            _logger.LogInformation("Закончили");
        }

        public async Task LoadAnnouncementsPhoneViewInfoAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Загрузка данных по расхлопам из Presto");

            IPrestoClient client = new PrestodbClient(Config);

            var query = @"
select * from apoturaev.hackaton_dataset2_stage5 where calltrackingid in (ID)
";

            var batch = 500;
            var batches = Math.Ceiling((decimal)calls.Length / batch);

            for (int i = 0; i < batches; i++)
            {
                var loadCount = Math.Min(batch, calls.Length - i * batch);

                var ids = calls.Skip(i * batch).Take(loadCount).Select(x => x.CalltrackingId);

                var stringIds = String.Join(',', ids.Select(x => x.ToString()).ToArray());

                var parsedQuery = query.Replace("ID", stringIds);

                var request = new ExecuteQueryV1Request(parsedQuery);

                var response = await client.ExecuteQueryV1(request);

                foreach (var data in response.Data)
                {
                    var call = calls.First(x => x.CalltrackingId == Int32.Parse(data[1].ToString()));
                    var announcementId = Int32.Parse(data[0].ToString());

                    var announcement = call.Announcements.FirstOrDefault(x => x.Id == announcementId);

                    if (announcement == default)
                    {
                        announcement = new AnnouncementInfo
                        {
                            Id = announcementId                            
                        };

                        call.Announcements.Add(announcement);
                    }

                    announcement.PhoneViewCount = Int32.Parse(data[2].ToString());

                }

                if (response.LastError != null)
                    _logger.LogError("Error while loading texts");

                _logger.LogInformation($"Batch {i} loaded");
            }

            _logger.LogInformation("Закончили");
        }

        public async Task LoadAnnouncementsAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Загрузка данных по объявлениям из Presto");

            IPrestoClient client = new PrestodbClient(Config);

            var query = @"
select * from apoturaev.hackaton_dataset2_stage3 where calltrackingid in (ID)
";

            var batch = 500;
            var batches = Math.Ceiling((decimal)calls.Length / batch);

            for (int i = 0; i < batches; i++)
            {
                var loadCount = Math.Min(batch, calls.Length - i * batch);

                var ids = calls.Skip(i * batch).Take(loadCount).Select(x => x.CalltrackingId);

                var stringIds = String.Join(',', ids.Select(x => x.ToString()).ToArray());

                var parsedQuery = query.Replace("ID", stringIds);

                var request = new ExecuteQueryV1Request(parsedQuery);

                var response = await client.ExecuteQueryV1(request);

                foreach (var data in response.Data)
                {
                    var call = calls.First(x => x.CalltrackingId == Int32.Parse(data[1].ToString()));
                    var announcementId = Int32.Parse(data[0].ToString());

                    var announcement = call.Announcements.FirstOrDefault(x => x.Id == announcementId);

                    if (announcement == default)
                    {
                        announcement = new AnnouncementInfo
                        {
                            Id = announcementId
                        };

                        call.Announcements.Add(announcement);
                    }                    
                }

                if (response.LastError != null)
                    _logger.LogError("Error while loading texts");

                _logger.LogInformation($"Batch {i} loaded");
            }

            _logger.LogInformation("Закончили");
        }


        #region DataPreparation
        public async System.Threading.Tasks.Task LoadAnnouncementsInfoAsync()
        {
            IPrestoClient Client = new PrestodbClient(Config);

            var day = new DateTime(2019, 7, 1);

            while (day < DateTime.Now)
            {
                var dateString = day.ToString("yyyy-MM-dd");
                var query = @"
        insert into apoturaev.hackaton_dataset2_stage2
        select
                json_extract_scalar(json_parse(geo.part), '$.name') as street_name, 
                ac.id, 
                cr.calltrackingid,
                json_extract_scalar(json_parse(ph.phone), '$.number') as phone                
        from 
                apoturaev.hackaton_dataset2_stage1 cr
        join 
                lst.announcement_change_v2 ac
        on 
                ac.userid = cr.realtyuserid             
        and
                ac.ptn_dadd = date 'DATE'
        cross join
                unnest(ac.geo_address) geo (part)
        cross join
                unnest(ac.phones) ph (phone)
        where     
                ac.wasactive = 1
        and 
                json_extract_scalar(json_parse(geo.part), '$.type') = 'street'        
        and
                cr.calldate between date 'DATE' and date 'DATE' + interval '1' day
        and
                cr.phone = json_extract_scalar(json_parse(ph.phone), '$.number')
";
                var parsed_query = query.Replace("DATE", dateString);

                var request = new ExecuteQueryV1Request(parsed_query);
                
                var response = await Client.ExecuteQueryV1(request);

                if (response.LastError == null)
                    _logger.LogInformation($"Date {dateString} loaded");

                day = day + TimeSpan.FromDays(1);
            }            
        }

        public async System.Threading.Tasks.Task LoadPhoneViewInfoAsync()
        {
            IPrestoClient Client = new PrestodbClient(Config);

            var day = new DateTime(2019, 7, 1);

            while (day < DateTime.Now)
            {
                var dateString = day.ToString("yyyy-MM-dd");
                var query = @"
        insert into apoturaev.hackaton_dataset2_stage3
        
        select
                ac.id, 
                cr.calltrackingid,
                cr.calldate
        from 
                apoturaev.hackaton_dataset2_stage1 cr
        join 
                lst.announcement_change_v2 ac
        on 
                ac.userid = cr.realtyuserid             
        and
                ac.ptn_dadd = date 'DATE'
        cross join
                unnest(ac.phones) ph (phone)
        where     
                ac.wasactive = 1
        and
                cr.calldate between date 'DATE' and date 'DATE' + interval '1' day
        and
                cr.phone = json_extract_scalar(json_parse(ph.phone), '$.number')        
";
                var parsed_query = query.Replace("DATE", dateString);

                var request = new ExecuteQueryV1Request(parsed_query);

                var response = await Client.ExecuteQueryV1(request);

                if (response.LastError == null)
                    _logger.LogInformation($"Date {dateString} loaded");

                day = day + TimeSpan.FromDays(1);
            }
        }

        public async System.Threading.Tasks.Task LoadHouseInfoAsync()
        {
            IPrestoClient Client = new PrestodbClient(Config);

            var day = new DateTime(2019, 8, 26);

            while (day < DateTime.Now)
            {
                var dateString = day.ToString("yyyy-MM-dd");
                var query = @"
        insert into apoturaev.hackaton_dataset2_stage8
        select
                ac.id, 
                cr.calltrackingid,
                cr.calldate,
                json_extract_scalar(json_parse(geo.part), '$.name') as house_name                 
        from 
                apoturaev.hackaton_dataset2_stage1 cr
        join 
                lst.announcement_change_v2 ac
        on 
                ac.userid = cr.realtyuserid             
        and
                ac.ptn_dadd = date 'DATE'
        cross join
                unnest(ac.geo_address) geo (part)
        cross join
                unnest(ac.phones) ph (phone)
        where     
                ac.wasactive = 1
        and 
                json_extract_scalar(json_parse(geo.part), '$.type') = 'house'        
        and
                cr.calldate between date 'DATE' and date 'DATE' + interval '1' day
        and
                cr.phone = json_extract_scalar(json_parse(ph.phone), '$.number')     
";
                var parsed_query = query.Replace("DATE", dateString);

                var request = new ExecuteQueryV1Request(parsed_query);

                var response = await Client.ExecuteQueryV1(request);

                if (response.LastError == null)
                    _logger.LogInformation($"Date {dateString} loaded");

                day = day + TimeSpan.FromDays(1);
            }
        }

        #endregion

    }
}
