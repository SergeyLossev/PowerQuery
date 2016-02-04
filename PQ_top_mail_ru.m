// (c) SergeyLossev

let
    src = Json.Document(Web.Contents("https://top.mail.ru/json/interestsdynamics.hc?id=1839501&amp;period=0&amp;date="&DateTime.ToText(DateTime.LocalNow(), "yyyy-MM-dd")&"&amp;gender=0&amp;agegroup=0&amp;pp=20&amp;sf=0&amp;aggregation=sum&amp;&sids=32,32/308,32/320,32/322,32/318,32/307,32/304,32/319,32/303,32/298,32/317,32/313,32/321,32/299,32/310,32/309,32/305,32/316,32/315,32/312,32/300,32/302,32/314,32/376,32/297,32/306,32/301,32/311,15,15/171,15/170,15/172,15/173,15/178,15/177,15/174,15/179,15/175,15/176,27,27/256,27/255,27/253,27/257,27/375,27/252,27/254,27/260,27/261,27/258,27/263,27/262,27/259,27/374,18,18/201,18/205,18/208,18/203,18/204,18/210,18/207,18/206,18/209,18/202,13,13/156,13/161,13/157,13/158,13/159,13/160,13/162,14,14/165,14/164,14/167,14/169,14/168,14/163,14/166,20,20/222,20/224,20/216,20/225,20/223,20/219,20/221,20/220,20/217,20/218,7,7/117,7/118,7/115,7/119,7/116,7/112,7/377,7/114,7/110,7/113,7/111,30,30/282,30/276,30/278,30/281,30/283,30/280,30/284,30/275,30/277,30/285,30/287,30/279,30/290,30/291,30/286,30/288,30/289,19,19/212,19/214,19/215,19/211,19/373,19/213,16,16/180,16/189,16/181,16/190,16/191,16/184,16/188,16/183,16/182,16/185,16/187,16/186,16/192,29,29/268,29/269,29/273,29/272,29/274,29/270,29/267,29/271,24,24/242,24/241,24/235,24/244,24/239,24/240,24/243,24/237,24/245,24/236,24/238,9,9/127,9/129,9/128,9/126,9/125,11,11/139,11/134,11/137,11/136,11/135,11/140,11/145,11/138,11/146,11/143,11/148,11/141,11/144,11/142,11/147,22,22/232,22/231,22/230,22/229,4,4/77,4/74,4/75,4/76,26,26/249,26/251,26/250,10,10/133,10/131,10/132,10/130,8,8/121,8/122,8/123,8/120,8/124,3,3/72,3/69,3/71,3/70,3/73,21,21/226,21/228,21/227,25,25/246,25/248,25/247,33,33/339,33/345,33/348,33/366,33/359,33/360,33/352,33/342,33/358,33/354,33/367,33/369,33/347,33/368,33/353,33/349,33/346,33/343,33/361,33/356,33/344,33/351,33/350,33/338,33/355,33/365,33/357,33/334,33/337,33/328,33/363,33/335,33/340,33/332,33/362,33/336,33/364,33/331,33/341,33/333,33/326,33/325,33/329,33/323,33/324,33/327,33/330,5,5/78,5/80,5/82,5/84,5/85,5/81,5/83,5/79,17,17/198,17/199,17/200,17/195,17/197,17/196,17/194,17/193,12,12/149,12/153,12/150,12/151,12/152,12/154,12/155,6,6/102,6/98,6/95,6/100,6/88,6/107,6/105,6/101,6/90,6/109,6/103,6/106,6/99,6/97,6/108,6/91,6/87,6/104,6/93,6/96,6/94,6/92,6/86,6/89,23,23/233,23/234&ytype=visitors&gtype=line&legend=1&_=0.36786172236315906")),
    series = src[series],
    tbl = Table.FromList(series, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    expand = Table.ExpandRecordColumn(tbl, "Column1", {"name", "sid", "data"}, {"name", "sid", "data"}),
    del_sid = Table.RemoveColumns(expand,{"sid"}),
    dump = Table.TransformColumns(del_sid,{{"data", each List.Transform(_, each _[y])}}),
    distinct_name = Table.Distinct(dump, {"name"}),
    t = Table.RenameColumns(distinct_name,{{"name", "Name"}, {"data", "Value"}}),

    sort_name = Table.Sort(t,{{"Name", Order.Ascending}}),
    index = Table.Buffer(Table.AddIndexColumn(sort_name, "Index", 0, 1)),
    list_names=List.Buffer(index[Name]),
	lenn=List.Count(list_names),
    namesX = List.Buffer(List.Combine(List.Transform(list_names, each List.Repeat({_}, lenn)))),
    namesY = List.Buffer(List.Repeat(list_names, lenn)),
    combine_XY = Table.Buffer(Table.FromColumns({namesX, namesY}, {"namesX", "namesY"})),
    add_corr = Table.Buffer(Table.AddColumn(combine_XY, "Custom", each 
let
	n=List.Count(index{[Name=[namesX]]}[Value]),
	corr=List.Covariance(index{[Name=[namesX]]}[Value], index{[Name=[namesY]]}[Value])/List.StandardDeviation(index{[Name=[namesX]]}[Value])/List.StandardDeviation(index{[Name=[namesY]]}[Value])*n/(n-1)

in
	corr
    )),
    pivot = Table.Pivot(add_corr, list_names, "namesX", "Custom"),
    join = Table.Join(pivot,{"namesY"},index,{"Name"},JoinKind.FullOuter),
    sort_index = Table.Sort(join,{{"Index", Order.Ascending}}),
    reorder_cols = Table.ReorderColumns(sort_index,{"Name", "namesY"}),
    del_cols = Table.RemoveColumns(reorder_cols,{"Value", "Index", "namesY"})
in
    del_cols
