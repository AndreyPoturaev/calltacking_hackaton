using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Hackaton.Loader;

namespace Hackaton
{
    public class Saver
    {
        public Saver(ILogger<Saver> logger)
        {
            _logger = logger;
        }


        private readonly ILogger _logger;

        public async Task SaveResultAsync(CallsInfo[] calls)
        {
            string fileName = @"C:\Users\a.poturaev\Desktop\hackaton\result.csv";

            _logger.LogInformation("Записываем значимый результат в csv");

            File.Delete(fileName);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("calltrackingid,realtyid");

            foreach (var call in calls)
            {
                var resId = call.CalculatedId == default ? "null" : call.CalculatedId.ToString();
                stringBuilder.AppendLine($"{call.CalltrackingId},{resId}");
            }
            await File.WriteAllTextAsync(fileName, stringBuilder.ToString(), Encoding.UTF8);

            _logger.LogInformation("Закончили");
        }


        public async Task SaveDumpAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Записываем все данные в json");

            if (GlobalSettings.CompareMode)
            {
                string positiveFileName = @"C:\Users\a.poturaev\Desktop\hackaton\result_dump_positive.json";
                File.Delete(positiveFileName);
                await File.WriteAllTextAsync(positiveFileName, JsonConvert.SerializeObject(calls.Where(x => x.CalculatedId == x.RazmetkaId)), Encoding.UTF8);

                string negativeFileName = @"C:\Users\a.poturaev\Desktop\hackaton\result_dump_negative.json";
                File.Delete(negativeFileName);
                await File.WriteAllTextAsync(negativeFileName, JsonConvert.SerializeObject(calls.Where(x => x.CalculatedId != x.RazmetkaId)), Encoding.UTF8);
            }
            else
            {
                string fileName = @"C:\Users\a.poturaev\Desktop\hackaton\result_dump.json";
             
                File.Delete(fileName);

                await File.WriteAllTextAsync(fileName, JsonConvert.SerializeObject(calls), Encoding.UTF8);                
            }

            _logger.LogInformation("Закончили");
        }

        public async Task SaveExperimentalDumpAsync(CallsInfo[] calls)
        {
            _logger.LogInformation("Сохраняем экспериментальный дамп в json");

            string fileName = @"C:\Users\a.poturaev\Desktop\hackaton\experimental_dump.json";

            File.Delete(fileName);

            await File.WriteAllTextAsync(fileName, JsonConvert.SerializeObject(calls), Encoding.UTF8);

            _logger.LogInformation("Закончили");
        }

        public async Task<CallsInfo[]> LoadExperimentalDumpAsync()
        {
            _logger.LogInformation("Загружаем экспериментальный дамп из json");

            string fileName = @"C:\Users\a.poturaev\Desktop\hackaton\experimental_dump.json";

            var data = File.ReadAllText(fileName, Encoding.UTF8);
            var result = JsonConvert.DeserializeObject<CallsInfo[]>(data);
            
            _logger.LogInformation("Закончили");

            return result;
        }
    }
}
