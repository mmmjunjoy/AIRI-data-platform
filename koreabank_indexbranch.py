import logging
from datetime import date
from enum import Enum
import requests
from bs4 import BeautifulSoup

from fastapi import APIRouter,Depends
from fastapi_utils.cbv import cbv 
from pydantic import BaseModel
from sqlalchemy import func,select,case,delete,insert
from sqlalchemy.ext.asyncio import AsyncSession

from common.db import db_etc
from models.models_etc import KoreabankIndexBranchly


router = APIRouter()

logger = logging.getLogger('uvicorn')


@cbv(router=router)

class koreagdpbranch:
    db:AsyncSession = Depends(db_etc.get_db)


    @router.get('koreagdpbranch',
        summary = '분기별 한국 gdp',
    )

    async def gdpkoreabranch(self):
        
        url = 'https://www.index.go.kr/unity/potal/eNara/sub/showStblGams3.do?stts_cd=273601&idx_cd=2736&freq=Q&period=196001:202203'
        
        header = {'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
            'Accept-Language' : 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding':'gzip, deflate, br'}
        data = { 'stts_cd':'273601','idx_cd':'2736','freq' :'Q' , 'period' : '196001:202203'}
        res = requests.get(url,headers=header,data=data)
        soup = BeautifulSoup(res.text,'html.parser')
        div = soup.find('div' , {'class':'con_table'})

        # 분기 select - branch
        ths = div.find_all('th',{'class':'tc'})
        
        resultbranch =[]
        for th in ths:

            itembranch = f'{th.text}' 

            resultbranch.append(itembranch)

        # 국내총생산 명목 GDP select -branch
        tbody = soup.find('tbody')
         
        trs = tbody.find('tr',{'id':'tr_273601_1'})
         
        resultnominalbranch=[]

        for tr in trs:

            tds = trs.find_all('td')
            
            for td in tds:
                 
                 itemnominalbranch = f'{td.text}'
                 
                 resultnominalbranch.append(itemnominalbranch)

        
        # 경제 성장률 GDP - branch
        trs2 = tbody.find('tr',{'id':'tr_273601_2'})
        
        resultrealbranch =[]

        for tr in trs2:

            tds = trs2.find_all('td')
            
            for td in tds:
                itemrealbranch = f'{td.text}'
                
                resultrealbranch.append(itemrealbranch)

        
        # 최종 종합
        # 
        res = []
        
        for branch,bnominal,breal in zip(resultbranch,resultnominalbranch,resultrealbranch):

            if branch[4:5]==str(1) :
                a ="-03-30"
            elif branch[4:5]==str(2) : 
                a ="-06-30"
            elif branch[4:5]==str(3) :
                a ="-09-30"
            elif branch[4:5]==str(4):
                a ="-12-31"

            item = {
                'date_branch':branch[:4]+ a,  #[:4] 위 방법 습득하기
                'nominal_gdp_branch':bnominal.replace(',',''),
                'real_gdp_branch':float(breal.replace('-','0'))
                
                }

            res.append(item)

        return res


    @router.post('/koreagdpbranchupdate',
        summary = 'koreagdpbranch 업데이트')


    async def updated_gdpkorea_branch(self):

        try:
            data=await self.gdpkoreabranch()

            await self.db.execute(
                insert(KoreabankIndexBranchly),
                data,
                )

            await self.db.commit()

            print('good')

        except Exception:

            await self.db.rollback()

            logger.exception('한국 년간 gdp branch 정보')

        return None


    async def get_origin_gdpkoreabranch(self):

        try:
            q=select(KoreabankIndexBranchly)
            rs =await self.db.execute(q)
            return rs.scalars().all()

        except Exception as e:
            print(e)

    async def koreabankbranchnewdata(self,original,new):
        for n in new:
            found=False

            for o in original:

                if n['date_branch'] == str(o.date_branch):
                    found=True

                    break
            
            if not found:
                n1 = KoreabankIndexBranchly(**n)
                self.db.add(n1)

                print(n['date_branch'] + "update success")


        print("finish")

    
    @router.post('/koreabankbranchlyupdated',
    
        summary='koreabank 분기별 업데이트')

    async def koreabankbranchlyupdated(self):

        print("start")

        try:
            new = await self.gdpkoreabranch()

            print('start2')

            original:list[KoreabankIndexBranchly] = await self.get_origin_gdpkoreabranch()

            print("start3")
            for o in original:

                found=False

                for n in new:

                    if str(o.date_branch) == n['date_branch']:

                        n1 = KoreabankIndexBranchly(**n)
                        o.date_branch = n1.date_branch
                        o.nominal_gdp_branch = n1.nominal_gdp_branch
                        o.real_gdp_branch = n1.real_gdp_branch
                        found =True

                        break

                if not found:
                    print(str(o.date_branch) + "check please")

            
            await self.koreabankbranchnewdata(original,new)
            await self.db.commit()

        except Exception as e:
            print(f'error:{e}')

            return None







        






            
















