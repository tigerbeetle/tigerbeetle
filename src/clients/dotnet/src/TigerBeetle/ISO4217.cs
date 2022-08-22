using System;

namespace TigerBeetle
{
    public sealed class ISO4217
    {
        #region Fields

        public static readonly ISO4217 AED = new(784, "AED", 2, "United Arab Emirates dirham");
        public static readonly ISO4217 AFN = new(971, "AFN", 2, "Afghan afghani");
        public static readonly ISO4217 ALL = new(008, "ALL", 2, "Albanian lek");
        public static readonly ISO4217 AMD = new(051, "AMD", 2, "Armenian dram");
        public static readonly ISO4217 ANG = new(532, "ANG", 2, "Netherlands Antillean guilder");
        public static readonly ISO4217 AOA = new(973, "AOA", 2, "Angolan kwanza");
        public static readonly ISO4217 ARS = new(032, "ARS", 2, "Argentine peso");
        public static readonly ISO4217 AUD = new(036, "AUD", 2, "Australian dollar");
        public static readonly ISO4217 AWG = new(533, "AWG", 2, "Aruban florin");
        public static readonly ISO4217 AZN = new(944, "AZN", 2, "Azerbaijani manat");
        public static readonly ISO4217 BAM = new(977, "BAM", 2, "Bosnia and Herzegovina convertible mark");
        public static readonly ISO4217 BBD = new(052, "BBD", 2, "Barbados dollar");
        public static readonly ISO4217 BDT = new(050, "BDT", 2, "Bangladeshi taka");
        public static readonly ISO4217 BGN = new(975, "BGN", 2, "Bulgarian lev");
        public static readonly ISO4217 BHD = new(048, "BHD", 3, "Bahraini dinar");
        public static readonly ISO4217 BIF = new(108, "BIF", 0, "Burundian franc");
        public static readonly ISO4217 BMD = new(060, "BMD", 2, "Bermudian dollar");
        public static readonly ISO4217 BND = new(096, "BND", 2, "Brunei dollar");
        public static readonly ISO4217 BOB = new(068, "BOB", 2, "Boliviano");
        public static readonly ISO4217 BOV = new(984, "BOV", 2, "Bolivian Mvdol");
        public static readonly ISO4217 BRL = new(986, "BRL", 2, "Brazilian real");
        public static readonly ISO4217 BSD = new(044, "BSD", 2, "Bahamian dollar");
        public static readonly ISO4217 BTN = new(064, "BTN", 2, "Bhutanese ngultrum");
        public static readonly ISO4217 BWP = new(072, "BWP", 2, "Botswana pula");
        public static readonly ISO4217 BYN = new(933, "BYN", 2, "Belarusian ruble");
        public static readonly ISO4217 BZD = new(084, "BZD", 2, "Belize dollar");
        public static readonly ISO4217 CAD = new(124, "CAD", 2, "Canadian dollar");
        public static readonly ISO4217 CDF = new(976, "CDF", 2, "Congolese franc");
        public static readonly ISO4217 CHE = new(947, "CHE", 2, "WIR euro (complementary currency)");
        public static readonly ISO4217 CHF = new(756, "CHF", 2, "Swiss franc");
        public static readonly ISO4217 CHW = new(948, "CHW", 2, "WIR franc (complementary currency)");
        public static readonly ISO4217 CLF = new(990, "CLF", 4, "Unidad de Fomento (funds code)");
        public static readonly ISO4217 CLP = new(152, "CLP", 0, "Chilean peso");
        public static readonly ISO4217 CNY = new(156, "CNY", 2, "Chinese yuan");
        public static readonly ISO4217 COP = new(170, "COP", 2, "Colombian peso");
        public static readonly ISO4217 COU = new(970, "COU", 2, "Unidad de Valor Real (UVR)");
        public static readonly ISO4217 CRC = new(188, "CRC", 2, "Costa Rican colon");
        public static readonly ISO4217 CUC = new(931, "CUC", 2, "Cuban convertible peso");
        public static readonly ISO4217 CUP = new(192, "CUP", 2, "Cuban peso");
        public static readonly ISO4217 CVE = new(132, "CVE", 2, "Cape Verdean escudo");
        public static readonly ISO4217 CZK = new(203, "CZK", 2, "Czech koruna");
        public static readonly ISO4217 DJF = new(262, "DJF", 0, "Djiboutian franc");
        public static readonly ISO4217 DKK = new(208, "DKK", 2, "Danish krone");
        public static readonly ISO4217 DOP = new(214, "DOP", 2, "Dominican peso");
        public static readonly ISO4217 DZD = new(012, "DZD", 2, "Algerian dinar");
        public static readonly ISO4217 EGP = new(818, "EGP", 2, "Egyptian pound");
        public static readonly ISO4217 ERN = new(232, "ERN", 2, "Eritrean nakfa");
        public static readonly ISO4217 ETB = new(230, "ETB", 2, "Ethiopian birr");
        public static readonly ISO4217 EUR = new(978, "EUR", 2, "Euro");
        public static readonly ISO4217 FJD = new(242, "FJD", 2, "Fiji dollar");
        public static readonly ISO4217 FKP = new(238, "FKP", 2, "Falkland Islands pound");
        public static readonly ISO4217 GBP = new(826, "GBP", 2, "Pound sterling");
        public static readonly ISO4217 GEL = new(981, "GEL", 2, "Georgian lari");
        public static readonly ISO4217 GHS = new(936, "GHS", 2, "Ghanaian cedi");
        public static readonly ISO4217 GIP = new(292, "GIP", 2, "Gibraltar pound");
        public static readonly ISO4217 GMD = new(270, "GMD", 2, "Gambian dalasi");
        public static readonly ISO4217 GNF = new(324, "GNF", 0, "Guinean franc");
        public static readonly ISO4217 GTQ = new(320, "GTQ", 2, "Guatemalan quetzal");
        public static readonly ISO4217 GYD = new(328, "GYD", 2, "Guyanese dollar");
        public static readonly ISO4217 HKD = new(344, "HKD", 2, "Hong Kong dollar");
        public static readonly ISO4217 HNL = new(340, "HNL", 2, "Honduran lempira");
        public static readonly ISO4217 HRK = new(191, "HRK", 2, "Croatian kuna");
        public static readonly ISO4217 HTG = new(332, "HTG", 2, "Haitian gourde");
        public static readonly ISO4217 HUF = new(348, "HUF", 2, "Hungarian forint");
        public static readonly ISO4217 IDR = new(360, "IDR", 2, "Indonesian rupiah");
        public static readonly ISO4217 ILS = new(376, "ILS", 2, "Israeli new shekel");
        public static readonly ISO4217 INR = new(356, "INR", 2, "Indian rupee");
        public static readonly ISO4217 IQD = new(368, "IQD", 3, "Iraqi dinar");
        public static readonly ISO4217 IRR = new(364, "IRR", 2, "Iranian rial");
        public static readonly ISO4217 ISK = new(352, "ISK", 0, "Icelandic króna");
        public static readonly ISO4217 JMD = new(388, "JMD", 2, "Jamaican dollar");
        public static readonly ISO4217 JOD = new(400, "JOD", 3, "Jordanian dinar");
        public static readonly ISO4217 JPY = new(392, "JPY", 0, "Japanese yen");
        public static readonly ISO4217 KES = new(404, "KES", 2, "Kenyan shilling");
        public static readonly ISO4217 KGS = new(417, "KGS", 2, "Kyrgyzstani som");
        public static readonly ISO4217 KHR = new(116, "KHR", 2, "Cambodian riel");
        public static readonly ISO4217 KMF = new(174, "KMF", 0, "Comoro franc");
        public static readonly ISO4217 KPW = new(408, "KPW", 2, "North Korean won");
        public static readonly ISO4217 KRW = new(410, "KRW", 0, "South Korean won");
        public static readonly ISO4217 KWD = new(414, "KWD", 3, "Kuwaiti dinar");
        public static readonly ISO4217 KYD = new(136, "KYD", 2, "Cayman Islands dollar");
        public static readonly ISO4217 KZT = new(398, "KZT", 2, "Kazakhstani tenge");
        public static readonly ISO4217 LAK = new(418, "LAK", 2, "Lao kip");
        public static readonly ISO4217 LBP = new(422, "LBP", 2, "Lebanese pound");
        public static readonly ISO4217 LKR = new(144, "LKR", 2, "Sri Lankan rupee");
        public static readonly ISO4217 LRD = new(430, "LRD", 2, "Liberian dollar");
        public static readonly ISO4217 LSL = new(426, "LSL", 2, "Lesotho loti");
        public static readonly ISO4217 LYD = new(434, "LYD", 3, "Libyan dinar");
        public static readonly ISO4217 MAD = new(504, "MAD", 2, "Moroccan dirham");
        public static readonly ISO4217 MDL = new(498, "MDL", 2, "Moldovan leu");
        public static readonly ISO4217 MGA = new(969, "MGA", 2, "Malagasy ariary");
        public static readonly ISO4217 MKD = new(807, "MKD", 2, "Macedonian denar");
        public static readonly ISO4217 MMK = new(104, "MMK", 2, "Myanmar kyat");
        public static readonly ISO4217 MNT = new(496, "MNT", 2, "Mongolian tögrög");
        public static readonly ISO4217 MOP = new(446, "MOP", 2, "Macanese pataca");
        public static readonly ISO4217 MRU = new(929, "MRU", 2, "Mauritanian ouguiya");
        public static readonly ISO4217 MUR = new(480, "MUR", 2, "Mauritian rupee");
        public static readonly ISO4217 MVR = new(462, "MVR", 2, "Maldivian rufiyaa");
        public static readonly ISO4217 MWK = new(454, "MWK", 2, "Malawian kwacha");
        public static readonly ISO4217 MXN = new(484, "MXN", 2, "Mexican peso");
        public static readonly ISO4217 MXV = new(979, "MXV", 2, "Mexican Unidad de Inversion (UDI)");
        public static readonly ISO4217 MYR = new(458, "MYR", 2, "Malaysian ringgit");
        public static readonly ISO4217 MZN = new(943, "MZN", 2, "Mozambican metical");
        public static readonly ISO4217 NAD = new(516, "NAD", 2, "Namibian dollar");
        public static readonly ISO4217 NGN = new(566, "NGN", 2, "Nigerian naira");
        public static readonly ISO4217 NIO = new(558, "NIO", 2, "Nicaraguan córdoba");
        public static readonly ISO4217 NOK = new(578, "NOK", 2, "Norwegian krone");
        public static readonly ISO4217 NPR = new(524, "NPR", 2, "Nepalese rupee");
        public static readonly ISO4217 NZD = new(554, "NZD", 2, "New Zealand dollar");
        public static readonly ISO4217 OMR = new(512, "OMR", 3, "Omani rial");
        public static readonly ISO4217 PAB = new(590, "PAB", 2, "Panamanian balboa");
        public static readonly ISO4217 PEN = new(604, "PEN", 2, "Peruvian sol");
        public static readonly ISO4217 PGK = new(598, "PGK", 2, "Papua New Guinean kina");
        public static readonly ISO4217 PHP = new(608, "PHP", 2, "Philippine peso");
        public static readonly ISO4217 PKR = new(586, "PKR", 2, "Pakistani rupee");
        public static readonly ISO4217 PLN = new(985, "PLN", 2, "Polish złoty");
        public static readonly ISO4217 PYG = new(600, "PYG", 0, "Paraguayan guaraní");
        public static readonly ISO4217 QAR = new(634, "QAR", 2, "Qatari riyal");
        public static readonly ISO4217 RON = new(946, "RON", 2, "Romanian leu");
        public static readonly ISO4217 RSD = new(941, "RSD", 2, "Serbian dinar");
        public static readonly ISO4217 RUB = new(643, "RUB", 2, "Russian ruble");
        public static readonly ISO4217 RWF = new(646, "RWF", 0, "Rwandan franc");
        public static readonly ISO4217 SAR = new(682, "SAR", 2, "Saudi riyal");
        public static readonly ISO4217 SBD = new(090, "SBD", 2, "Solomon Islands dollar");
        public static readonly ISO4217 SCR = new(690, "SCR", 2, "Seychelles rupee");
        public static readonly ISO4217 SDG = new(938, "SDG", 2, "Sudanese pound");
        public static readonly ISO4217 SEK = new(752, "SEK", 2, "Swedish krona");
        public static readonly ISO4217 SGD = new(702, "SGD", 2, "Singapore dollar");
        public static readonly ISO4217 SHP = new(654, "SHP", 2, "Saint Helena pound");
        public static readonly ISO4217 SLL = new(694, "SLL", 2, "Sierra Leonean leone");
        public static readonly ISO4217 SOS = new(706, "SOS", 2, "Somali shilling");
        public static readonly ISO4217 SRD = new(968, "SRD", 2, "Surinamese dollar");
        public static readonly ISO4217 SSP = new(728, "SSP", 2, "South Sudanese pound");
        public static readonly ISO4217 STN = new(930, "STN", 2, "São Tomé and Príncipe dobra");
        public static readonly ISO4217 SVC = new(222, "SVC", 2, "Salvadoran colón");
        public static readonly ISO4217 SYP = new(760, "SYP", 2, "Syrian pound");
        public static readonly ISO4217 SZL = new(748, "SZL", 2, "Swazi lilangeni");
        public static readonly ISO4217 THB = new(764, "THB", 2, "Thai baht");
        public static readonly ISO4217 TJS = new(972, "TJS", 2, "Tajikistani somoni");
        public static readonly ISO4217 TMT = new(934, "TMT", 2, "Turkmenistan manat");
        public static readonly ISO4217 TND = new(788, "TND", 3, "Tunisian dinar");
        public static readonly ISO4217 TOP = new(776, "TOP", 2, "Tongan paʻanga");
        public static readonly ISO4217 TRY = new(949, "TRY", 2, "Turkish lira");
        public static readonly ISO4217 TTD = new(780, "TTD", 2, "Trinidad and Tobago dollar");
        public static readonly ISO4217 TWD = new(901, "TWD", 2, "New Taiwan dollar");
        public static readonly ISO4217 TZS = new(834, "TZS", 2, "Tanzanian shilling");
        public static readonly ISO4217 UAH = new(980, "UAH", 2, "Ukrainian hryvnia");
        public static readonly ISO4217 UGX = new(800, "UGX", 0, "Ugandan shilling");
        public static readonly ISO4217 USD = new(840, "USD", 2, "United States dollar");
        public static readonly ISO4217 USN = new(997, "USN", 2, "United States dollar (next day)");
        public static readonly ISO4217 UYI = new(940, "UYI", 0, "Uruguay Peso en Unidades Indexadas (URUIURUI)");
        public static readonly ISO4217 UYU = new(858, "UYU", 2, "Uruguayan peso");
        public static readonly ISO4217 UYW = new(927, "UYW", 4, "Unidad previsional");
        public static readonly ISO4217 UZS = new(860, "UZS", 2, "Uzbekistan");
        public static readonly ISO4217 VED = new(926, "VED", 2, "Venezuelan bolívar digital");
        public static readonly ISO4217 VES = new(928, "VES", 2, "Venezuelan bolívar soberano");
        public static readonly ISO4217 VND = new(704, "VND", 0, "Vietnamese đồng");
        public static readonly ISO4217 VUV = new(548, "VUV", 0, "Vanuatu vatu");
        public static readonly ISO4217 WST = new(882, "WST", 2, "Samoan tala");
        public static readonly ISO4217 XAF = new(950, "XAF", 0, "CFA franc BEAC");
        public static readonly ISO4217 XAG = new(961, "XAG", 0, "Silver (one troy ounce)");
        public static readonly ISO4217 XAU = new(959, "XAU", 0, "Gold (one troy ounce)");
        public static readonly ISO4217 XBA = new(955, "XBA", 0, "European Composite Unit (EURCO)");
        public static readonly ISO4217 XBB = new(956, "XBB", 0, "European Monetary Unit (E.M.U.-6)");
        public static readonly ISO4217 XBC = new(957, "XBC", 0, "European Unit of Account 9 (E.U.A.-9)");
        public static readonly ISO4217 XBD = new(958, "XBD", 0, "European Unit of Account 17 (E.U.A.-17)");
        public static readonly ISO4217 XCD = new(951, "XCD", 2, "East Caribbean dollar");
        public static readonly ISO4217 XDR = new(960, "XDR", 0, "Special drawing rights");
        public static readonly ISO4217 XOF = new(952, "XOF", 0, "CFA franc BCEAO");
        public static readonly ISO4217 XPD = new(964, "XPD", 0, "Palladium (one troy ounce)");
        public static readonly ISO4217 XPF = new(953, "XPF", 0, "CFP franc");
        public static readonly ISO4217 XPT = new(962, "XPT", 0, "Platinum (one troy ounce)");
        public static readonly ISO4217 XSU = new(994, "XSU", 0, "SUCRE");
        public static readonly ISO4217 XTS = new(963, "XTS", 0, "Code reserved for testing");
        public static readonly ISO4217 XUA = new(965, "XUA", 0, "ADB Unit of Account");
        public static readonly ISO4217 XXX = new(999, "XXX", 0, "No currency");
        public static readonly ISO4217 YER = new(886, "YER", 2, "Yemeni rial");
        public static readonly ISO4217 ZAR = new(710, "ZAR", 2, "South African rand");
        public static readonly ISO4217 ZMW = new(967, "ZMW", 2, "Zambian kwacha");
        public static readonly ISO4217 ZWL = new(932, "ZWL", 2, "Zimbabwean dollar");

        private readonly decimal factor;

        #endregion Fields

        #region Constructor

        public ISO4217(uint code, string name, int decimalPlaces, string description)
        {
            factor = (decimal)Math.Pow(10, decimalPlaces);

            Name = name;
            Code = code;
            DecimalPlaces = decimalPlaces;
            Description = description;
        }

        #endregion Constructor

        #region Properties

        public uint Code { get; }

        public string Name { get; }

        public int DecimalPlaces { get; }

        public string Description { get; }

        #endregion Properties

        #region Methods

        public override string ToString()
        {
            return $"{Name} {Description}";
        }

        public ulong ToUInt64(decimal value) => (ulong)(value * factor);

        public decimal FromUInt64(ulong value) => Math.Round(value / factor, DecimalPlaces);

        #endregion Methods
    }
}
