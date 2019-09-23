using F23.StringSimilarity;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Hackaton.Loader;

namespace Hackaton
{
    public class Analyzer
    {
        public Analyzer(ILogger<Analyzer> logger)
        {
            _logger = logger;            
        }


        private readonly ILogger _logger;      
        

        public void OneAnnouncementAnalysis(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ на единственную объявку");

            var oneAnnouncements = calls.Where(x => x.Announcements.Count == 1);

            foreach (var call in oneAnnouncements)
                call.Announcements.Single().Features.Add(new FeatureInfo
                {
                    Name = FeatureInfo.ONE_ANNOUNEMENT,
                    Weight = 1.0f
                });
            _logger.LogInformation("Закончили");
        }

        public void PhoneViewAnalysis(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ по расхлопам");

            foreach (var call in calls)
            {
                AnnouncementInfo announcementWithView = default;
                var announcementWithViewCount = 0;

                foreach (var announcement in call.Announcements)
                {
                    if (announcement.PhoneViewCount != default)
                    {
                        announcement.Features.Add(new FeatureInfo
                        {
                            Name = FeatureInfo.PHONE_VIEW,
                            Weight = (double)announcement.PhoneViewCount / call.Announcements.Sum(x => x.PhoneViewCount)
                        }); 

                        announcementWithView = announcement;
                        announcementWithViewCount++;
                    }
                }

                if (announcementWithViewCount == 1)
                    announcementWithView.Features.Add(new FeatureInfo
                    {
                        Name = FeatureInfo.ONE_PHONE_VIEW,
                        Weight = 1.0f
                    });
            }
            _logger.LogInformation("Закончили");
        }        

        public void TextNormalization(CallsInfo[] calls)
        {
            _logger.LogInformation("Нормализация текста");

            var adverbs = File.ReadAllLines(@"C:\Users\a.poturaev\Desktop\hackaton\adverbs.csv").Select(x => x.ToLower()).ToArray();

            foreach (var call in calls)
            {
                if (call.Text == null)
                    continue;

                var text = call.Text;                
                var words = text.Split(' ', '.', ',');
                var big_words = words.Where(x => x != "</s>").Where(x =>
                {
                    if (x.Length <= 2)
                    {
                        if (Int32.TryParse(x, out int number))
                            return true;
                        return false;
                    }
                    else
                        return true;
                }).ToArray();
                var filtered_words = big_words.Where(x => !adverbs.Contains(x.ToLower())).ToArray();
                call.Text = String.Join(' ', filtered_words);
            }

            _logger.LogInformation("Закончили");
        }

        public void HouseAnalysis(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ наличия номера дома");

            var l = new NormalizedLevenshtein();

            foreach (var call in calls)
            {
                if (call.Text == null)
                    continue;

                var words = call.Text.Split(' ');

                foreach (var announcement in call.Announcements)
                {
                    if (announcement.House == default)
                        continue;

                    double max_similarity = default;
                    string best_word = default;

                    foreach (var word in words)
                    {
                        var similarity = l.Similarity(word.ToLower(), announcement.House.ToLower());
                        if (similarity >= max_similarity)
                        {
                            best_word = word;
                            max_similarity = similarity;
                        }
                    }

                    //if (max_similarity == 1)
                    {
                        announcement.Features.Add(new FeatureInfo
                        {
                            Name = FeatureInfo.HOUSE,
                            Weight = max_similarity,
                            Data = best_word ?? ""
                        });
                    }
                }
            }

            _logger.LogInformation("Закончили");
        }

        public void NormalizedLevenshteinAnalysis(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ похожих методом NormalizedLevenshtein");

            var l = new NormalizedLevenshtein();

            foreach (var call in calls)
            {
                if (call.Text == null)
                    continue;

                var words = call.Text.Split(' ');

                foreach (var announcement in call.Announcements)
                {
                    if (announcement.Street == default)
                        continue;

                    double max_similarity = default;
                    string best_word = default;

                    foreach (var word in words)
                    {
                        var similarity = l.Similarity(word.ToLower(), announcement.Street.ToLower());
                        if (similarity >= max_similarity)
                        {
                            best_word = word;
                            max_similarity = similarity;
                        }
                    }

                    announcement.Features.Add(new FeatureInfo
                    {
                        Name = FeatureInfo.LEVENSTAIN,
                        Weight = max_similarity,
                        Data = best_word ?? ""
                    });
                }                
            }

            _logger.LogInformation("Закончили");
        }

        public void CosineAnalysis(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ похожих методом Cosine");

            var l = new Cosine();

            foreach (var call in calls)
            {
                if (call.Text == null)
                    continue;

                var words = call.Text.Split(' ');

                foreach (var announcement in call.Announcements)
                {
                    if (announcement.Street == default)
                        continue;

                    double max_similarity = default;
                    string best_word = default;

                    foreach (var word in words)
                    {
                        var similarity = l.Similarity(word.ToLower(), announcement.Street.ToLower());
                        if (similarity >= max_similarity)
                        {
                            best_word = word;
                            max_similarity = similarity;
                        }
                    }

                    announcement.Features.Add(new FeatureInfo
                    {
                        Name = FeatureInfo.COSINE,
                        Weight = max_similarity,
                        Data = best_word ?? ""
                    });
                }
            }

            _logger.LogInformation("Закончили");
        }

        public void JaroWinklerAnalisys(CallsInfo[] calls)
        {
            _logger.LogInformation("Анализ похожих методом JaroWinkler");

            var l = new JaroWinkler();

            foreach (var call in calls)
            {
                if (call.Text == null)
                    continue;

                var words = call.Text.Split(' ');

                foreach (var announcement in call.Announcements)
                {
                    if (announcement.Street == default)
                        continue;

                    double max_similarity = default;
                    string best_word = default;

                    foreach (var word in words)
                    {
                        var similarity = l.Similarity(word.ToLower(), announcement.Street.ToLower());
                        if (similarity >= max_similarity)
                        {
                            best_word = word;
                            max_similarity = similarity;
                        }
                    }

                    announcement.Features.Add(new FeatureInfo
                    {
                        Name = FeatureInfo.JARO,
                        Weight = max_similarity,
                        Data = best_word ?? ""
                    });
                }
            }

            _logger.LogInformation("Закончили");
        }


        private double GetFeatureWeight(AnnouncementInfo info, string name)
        {
            var feature = info.Features.FirstOrDefault(x => x.Name == name);
            if (feature == default)
                return 0;

            return feature.Weight;            
        }

        public void CalculateResult(CallsInfo[] calls)
        {
            _logger.LogInformation("Расчёт результатов");

            foreach (var call in calls)
            {
                if (call.Announcements.Count == 0)
                    continue;

                AnnouncementInfo bestChoose = default;

                foreach (var announcement in call.Announcements)
                {
                    var oneAnn = GetFeatureWeight(announcement, FeatureInfo.ONE_ANNOUNEMENT);
                    var leven = GetFeatureWeight(announcement, FeatureInfo.LEVENSTAIN);
                    var cosine = GetFeatureWeight(announcement, FeatureInfo.COSINE);
                    var jaro = GetFeatureWeight(announcement, FeatureInfo.JARO);
                    var house = GetFeatureWeight(announcement, FeatureInfo.HOUSE);
                    var phone = GetFeatureWeight(announcement, FeatureInfo.PHONE_VIEW);
                    
                    var featureWeights = new List<double>
                    {
                        oneAnn * 1
                        ,leven * 0.79 + house * 0.2
                        ,cosine * 0.79 + house * 0.2
                        ,jaro * 0.79 + house * 0.2 
                        ,phone * 1                       
                    };

                    var weight = featureWeights.Max();

                    announcement.Similarity = weight;

                    if (bestChoose == null || bestChoose.Similarity <= announcement.Similarity)
                        bestChoose = announcement;                    
                }
                
                call.CalculatedId = bestChoose.Id;
                call.Similarity = bestChoose.Similarity;
            }

            _logger.LogInformation("Закончили");
        }

        public void ShowResult(CallsInfo[] calls)
        {
            _logger.LogInformation("Результаты:");

            var withoutAnnouncements = calls.Count(x => x.Announcements.Count == 0);
            var withAnnouncements = calls.Count(x => x.Announcements.Count != 0);
            var oneAnnouncements = calls.Count(x => x.Announcements.Any(y => y.Features.Any(z => z.Name == FeatureInfo.ONE_ANNOUNEMENT)));
            var onePhoneView = calls.Count(x => x.Announcements.Any(y => y.Features.Any(z => z.Name == FeatureInfo.ONE_PHONE_VIEW)));
            var withHouse = calls.Count(x => x.Announcements.Any(y => y.Features.Any(z => z.Name == FeatureInfo.HOUSE)));
            var withoutText = calls.Count(x => x.Text == default);

            _logger.LogWarning($"Without announcements: {withoutAnnouncements} ");
            _logger.LogWarning($"With announcements: {withAnnouncements}");
            _logger.LogWarning($"One announcement: {oneAnnouncements}");
            _logger.LogWarning($"One phone view: {onePhoneView}");
            _logger.LogWarning($"With house: {withHouse}");
            _logger.LogWarning($"Without text: {withoutText}");


            var mappedCalls = GlobalSettings.CompareMode ? calls.Where(x => x.RazmetkaId != default) : calls;         
            var ok = mappedCalls.Count(x => x.CalculatedId == x.RazmetkaId);
            var fail = mappedCalls.Count(x => x.CalculatedId != x.RazmetkaId);

            var total = mappedCalls.Count();

            var similarity_0_6 = mappedCalls.Count(x => x.Similarity > 0.6);
            var similarity_0_7 = mappedCalls.Count(x => x.Similarity > 0.7);
            var similarity_0_8 = mappedCalls.Count(x => x.Similarity > 0.8);
            var similarity_0_9 = mappedCalls.Count(x => x.Similarity > 0.9);

            if (GlobalSettings.CompareMode == true)
            {
                _logger.LogInformation($"Mapped, %: { ok * 100.0 / total }");
                _logger.LogInformation($"Not Recognized, %: { fail * 100.0 / total }");
            }
            _logger.LogInformation($"Similarity > 0.6, %: { similarity_0_6 * 100 / total }");
            _logger.LogInformation($"Similarity > 0.7, %: { similarity_0_7 * 100 / total }");
            _logger.LogInformation($"Similarity > 0.8, %: { similarity_0_8 * 100 / total }");
            _logger.LogInformation($"Similarity > 0.9, %: { similarity_0_9 * 100 / total }");         
        }

        public async Task AnalyzeThisAsync(CallsInfo[] calls)
        {            
            OneAnnouncementAnalysis(calls);

            PhoneViewAnalysis(calls);

            TextNormalization(calls);

            HouseAnalysis(calls);
            
            NormalizedLevenshteinAnalysis(calls);
                       
            CosineAnalysis(calls);

            JaroWinklerAnalisys(calls);               

            CalculateResult(calls);

            ShowResult(calls);     
            
        }
    }
}
