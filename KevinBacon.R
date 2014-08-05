# hpcc.begin(import='IMDB')

#For Filtering Data
hpcc.string.find('condn1','IMDB.FileActors.moviename','Boffo',instance=1,output=0)
hpcc.string.find('condn2','IMDB.FileActors.moviename','Slasher Film',instance=1,output=0)

condn1 <- condition(field='condn1',operator='=',keyword=0)
condn2 <- condition(field='condn2',operator='=',keyword=0)
condn3 <- condition(field='actorname',operator='!=','',type='STRING')
condn3 <- condition(field='actorname',operator='!=','',type='STRING')
condn4 <- condition(field='movie_type',operator='!=',"Video",type='STRING')
condn5 <- condition(field='isTVSeries',operator='=','N',type='STRING')
condn6 <- condition(field='movie_type',operator='!=',"For TV",type='STRING')

cond <- condition(field=condn1,operator='AND',keyword=condn2,type='NUM')
cond <- condition(field=cond,operator='AND',keyword=condn3,type='NUM')
cond <- condition(field=cond,operator='AND',keyword=condn4,type='NUM')
cond <- condition(field=cond,operator='AND',keyword=condn5,type='NUM')
cond <- condition(field=cond,operator='AND',keyword=condn6,type='NUM')

hpcc.filter(data='IMDB.FileActors',condition=cond,out.dataframe='ds_IMDB',output=0)
#filter End

hpcc.define.record('slim_IMDB_rec',c('STRING','STRING150'),c('actor','movie'))
xd <- hpcc.string.replace(out.dataframe='SELF.actor','L.actorname','(I)','',submit=FALSE)
hpcc.define.transform(returnType='slim_IMDB_rec',transformName='slim_it',argTypes='ds_IMDB',argNames='L',xd,'SELF.movie := L.moviename;')
hpcc.project(dataframe='ds_IMDB',calltransfunc='slim_it',out.dataframe='ActorsInMovies',output=0)

hpcc.string.find(out.dataframe='cdnsECL1','ActorsInMovies.actor','Kevin',instance=1,output=0)
cdn1 <- condition('cdnsECL1',operator='>',0)
hpcc.string.find(out.dataframe='cdnsECL2','ActorsInMovies.actor','Bacon',instance=1,output=0)
cdn2 <- condition('cdnsECL2',operator='>',0)

hpcc.filter(out.dataframe='AllKBEntries',condition=condition(cdn1,operator='AND',keyword=cdn2,type='NUM'),data='ActorsInMovies',output=0)

hpcc.dedup(output=0,dataframe='AllKBEntries',out.dataframe='KBMovies',condition='movie',all='ALL')

hpcc.filter(output=0,data='ActorsInMovies',out.dataframe='CoStars',condition='Movie IN SET(KBMovies,Movie)')

hpcc.filter(output=0,data='CoStars',condition="actor<>'Kevin Bacon'",out.dataframe='CoStars2')
hpcc.dedup(output=0,data='CoStars2',out.dataframe='KBCoStars',condition='actor',all='ALL')

hpcc.join(output=0,out.dataframe='CSM2',Dataset1='ActorsInMovies',Dataset2='KBCoStars',joinCondition='LEFT.actor=RIGHT.actor',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,dataframe='CSM2',out.dataframe='CSM',condition='movie',all='ALL')

hpcc.filter(output=0,data='CSM',out.dataframe='KBCoStarMovies',condition='movie NOT IN SET(KBMovies,movie)')
hpcc.join(output=0,out.dataframe='KBCo2S2',Dataset1='ActorsInMovies',Dataset2='KBCoStarMovies',joinCondition='LEFT.movie=RIGHT.movie',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,dataframe='KBCo2S2',condition='actor',all='ALL',out.dataframe='KBCo2S')

hpcc.join(output=0,out.dataframe='KBCo2Stars',Dataset1='KBCo2S',Dataset2='KBCoStars',joinCondition='LEFT.actor=RIGHT.actor',fields='KBCo2S',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='Co2SM2',Dataset1='ActorsInMovies',Dataset2='KBCo2Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='Co2SM',dataframe='Co2SM2',condition='movie',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo2StarMovies',Dataset1='Co2SM',Dataset2='KBCoStarMovies',joinCondition='Co2SM.movie=KBCoStarMovies.movie',fields='LEFT',type='LEFT ONLY')


hpcc.join(output=0,out.dataframe='KBCo3S2',Dataset1='ActorsInMovies',Dataset2='KBCo2StarMovies',joinCondition='ActorsInMovies.movie=KBCo2StarMovies.movie',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='KBCo3S',dataframe='KBCo3S2',condition='actor',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo3Stars',Dataset1='KBCo3S',Dataset2='KBCo2Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='KBCo3S',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='Co3SM2',Dataset1='ActorsInMovies',Dataset2='KBCo3Stars',joinCondition='ActorsInMovies.actor=KBCo3Stars.actor',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='Co3SM',dataframe='Co3SM2',condition='movie',all='ALL')

hpcc.join(output=0,out.dataframe='KBCo3StarMovies',Dataset1='Co3SM',Dataset2='KBCo2StarMovies',joinCondition='Co3SM.movie=KBCo2StarMovies.movie',fields='Co3SM',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='KBCo4S2',Dataset1='ActorsInMovies',Dataset2='KBCo3StarMovies',joinCondition='ActorsInMovies.movie=KBCo4StarMovies.movie',fields='ActorsInMovies',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='KBCo4S',dataframe='KBCo4S2',condition='actor',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo4Stars',Dataset1='KBCo4S',Dataset2='KBCo3Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='KBCo4S',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='Co4SM2',Dataset1='ActorsInMovies',Dataset2='KBCo4Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='LEFT',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='Co4SM',dataframe='Co4SM2',condition='movie',all='ALL')

hpcc.join(output=0,out.dataframe='KBCo4StarMovies',Dataset1='Co4SM',Dataset2='KBCo3StarMovies',joinCondition='Co4SM.movie=KBCo3StarMovies.movie',fields='LEFT',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='KBCo5S2',Dataset1='ActorsInMovies',Dataset2='KBCo4StarMovies',joinCondition='LEFT.movie=RIGHT.movie',fields='LEFT',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='KBCo5S',dataframe='KBCo5S2',condition='actor',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo5Stars',Dataset1='KBCo5S',Dataset2='KBCo4Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='LEFT',type='LEFT ONLY')

hpcc.join(output=0,out.dataframe='Co5SM2',Dataset1='ActorsInMovies',Dataset2='KBCo5Stars',joinCondition='LEFT.actor=RIGHT.actor',fields='LEFT',type='LOOKUP')
hpcc.dedup(output=0,out.dataframe='Co5SM',dataframe='Co5SM2',condition='movie',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo5StarMovies',Dataset1='Co5SM',Dataset2='KBCo4StarMovies',joinCondition='LEFT.movie=RIGHT.movie',fields='LEFT',type='LEFT ONLY')

hpcc.filter(output=0,out.dataframe='KBCo6S2',data='ActorsInMovies',condition='movie IN SET(KBCo5StarMovies, movie)')
hpcc.dedup(output=0,out.dataframe='KBCo6S',dataframe='KBCo6S2',condition='actor',all='ALL')
hpcc.join(output=0,out.dataframe='KBCo6Stars',Dataset1='KBCo6S',Dataset2='KBCo5Stars',joinCondition='KBCo6S.actor=KBCo5Stars.actor',fields='LEFT',type='LEFT ONLY')

hpcc.filter(output=0,out.dataframe='Co6SM2',data='ActorsInMovies',condition='actor IN SET(KBCo6Stars, actor)',)
hpcc.dedup(output=0,out.dataframe='Co6SM',dataframe='Co6SM2',condition='movie',all='ALL')
hpcc.filter(output=0,out.dataframe='KBCo6StarMovies',data='Co6SM',condition='movie NOT IN SET(KBCo5StarMovies, movie)')

hpcc.filter(output=0,out.dataframe='KBCo7S2',data='ActorsInMovies',condition='movie IN SET(KBCo6StarMovies,movie)')
hpcc.dedup(output=0,out.dataframe='KBCo7S',dataframe='KBCo7S2',condition='actor',all='ALL')
hpcc.filter(out.dataframe='KBCo7Stars',data='KBCo7S',condition='actor NOT IN SET(KBCo6Stars, actor)')

hpcc.count('KBMoviesCount',in.data='KBMovies')
hpcc.count('KBCoStarsCount',in.data='KBCoStars')
hpcc.count('KBCoStarMoviesCount',in.data='KBCoStarMovies')
hpcc.count('KBCo2StarsCount',in.data='KBCo2Stars')
hpcc.count('KBCo2StarMoviesCount',in.data='KBCo2StarMovies')
hpcc.count('KBCo3StarsCount',in.data='KBCo3Stars')
hpcc.count('KBCo3StarMoviesCount',in.data='KBCo3StarMovies')
hpcc.count('KBCo4StarsCount',in.data='KBCo4Stars')
hpcc.count('KBCo4StarMoviesCount',in.data='KBCo4StarMovies')
hpcc.count('KBCo5StarsCount',in.data='KBCo5Stars')
hpcc.count('KBCo5StarMoviesCount',in.data='KBCo5StarMovies')
hpcc.count('KBCo6StarsCount',in.data='KBCo6Stars')
hpcc.count('KBCo7StarsCount',in.data='KBCo7Stars')
hpcc.count('KBCo6StarMoviesCount',in.data='KBCo6StarMovies')
