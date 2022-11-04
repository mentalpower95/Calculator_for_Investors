from sqlalchemy import Column, String, Float, create_engine, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv


def add_base(file, table, query):
    with open(file) as file_lines:
        reader = csv.DictReader(file_lines, delimiter=',')
        for line in reader:
            for key, value in line.items():
                if value == "":
                    line[key] = None
            if not bool(query.filter(table.ticker == f'''{line['ticker']}''').first()):
                session.add(table(**line))
        session.commit()


Base = declarative_base()


class Companies(Base):
    __tablename__ = 'companies'

    ticker = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)


class Financial(Base):
    __tablename__ = 'financial'

    ticker = Column(String, primary_key=True)
    ebitda = Column(Float)
    sales = Column(Float)
    net_profit = Column(Float)
    market_price = Column(Float)
    net_debt = Column(Float)
    assets = Column(Float)
    equity = Column(Float)
    cash_equivalents = Column(Float)
    liabilities = Column(Float)


engine = create_engine('sqlite:///investor.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
query_c, query_f = session.query(Companies), session.query(Financial)

add_base('financial.csv', Financial, query_f)
add_base('companies.csv', Companies, query_c)


class CalculatorInvestor:

    def main_menu(self):
        print('Welcome to the Investor Program!')
        while True:
            print('\nMAIN MENU', '0 Exit', '1 CRUD operations', '2 Show top ten companies by criteria',
                  '\nEnter an option:', sep='\n')
            choice = input()
            if choice == '0':
                print('Have a nice day!')
                exit()
            elif choice == '1':
                self.crud_menu()
            elif choice == '2':
                self.top_ten_menu()
            else:
                print('Invalid option!')

    def crud_menu(self):
        print('\nCRUD MENU', '0 Back', '1 Create a company', '2 Read a company', '3 Update a company',
              '4 Delete a company', '5 List all companies\n', 'Enter an option:', sep='\n')
        choice = input()
        if choice == '1':
            self.creating_company()
        elif choice == '2':
            self.read_company(query_c, query_f)
        elif choice == '3':
            self.update_company(query_c, query_f)
        elif choice == '4':
            self.delete_company(query_c, query_f)
        elif choice == '5':
            self.list_all_companies(query_c)
        else:
            print('Not implemented!')

    def top_ten_menu(self):
        print('\nTOP TEN MENU', '0 Back', '1 List by ND/EBITDA', '2 List by ROE', '3 List by ROA\n',
              'Enter an option:', sep='\n')
        choice = input()
        if choice == 0:
            print('\n')
        elif choice == '1':
            self.top_nd()
        elif choice == '2':
            self.top_roe()
        elif choice == '3':
            self.top_roa()
        else:
            print('Invalid option!')

    def creating_company(self):
        new_company, new_financial = dict(), dict()
        new_company['ticker'] = new_financial['ticker'] = input('''Enter ticker (in the format 'MOON'):''')
        new_company['name'] = input('''Enter company (in the format 'Moon Corp'):''')
        new_company['sector'] = input('''Enter industries (in the format 'Technology'):''')
        self.new_financial(new_financial)
        session.add(Financial(**new_financial)), session.add(Companies(**new_company))
        session.commit()
        print('Company created successfully!')

    @staticmethod
    def new_financial(financial_dict):
        financial_dict['ebitda'] = input('''Enter ebitda (in the format '987654321'):''')
        financial_dict['sales'] = input('''Enter sales (in the format '987654321'):''')
        financial_dict['net_profit'] = input('''Enter net profit (in the format '987654321'):''')
        financial_dict['market_price'] = input('''Enter market price (in the format '987654321'):''')
        financial_dict['net_debt'] = input('''Enter net dept (in the format '987654321'):''')
        financial_dict['assets'] = input('''Enter assets (in the format '987654321'):''')
        financial_dict['equity'] = input('''Enter equity (in the format '987654321'):''')
        financial_dict['cash_equivalents'] = input('''Enter cash equivalents (in the format '987654321'):''')
        financial_dict['liabilities'] = input('''Enter liabilities (in the format '987654321'):''')

    def read_company(self, query1, query2):
        chosen_company = self.select_company(query1)
        if chosen_company:
            print(*chosen_company, sep=" ")
            for row in query2.filter(Financial.ticker == f'{chosen_company[0]}'):
                print(f'P/E = {self.calc_finances(row.market_price, row.net_profit)}',
                      f'P/S = {self.calc_finances(row.market_price, row.sales)}',
                      f'P/B = {self.calc_finances(row.market_price, row.assets)}',
                      f'ND/EBITDA = {self.calc_finances(row.net_debt, row.ebitda)}',
                      f'ROE = {self.calc_finances(row.net_profit, row.equity)}',
                      f'ROA = {self.calc_finances(row.net_profit, row.assets)}',
                      f'L/A = {self.calc_finances(row.liabilities, row.assets)}', sep='\n')

    @staticmethod
    def select_company(query):
        company_name = input('Enter company name:')
        chosen_companies = []
        for n, cmp in enumerate(query.filter(Companies.name.like(f'%{company_name}%'))):
            chosen_companies.append([cmp.ticker, cmp.name])
            print(n, cmp.name)
        if len(chosen_companies) == 0:
            print('Company not found!')
            return False
        else:
            company_number = int(input('Enter company number:'))
            return chosen_companies[company_number]

    @staticmethod
    def calc_finances(first, second):
        if first is None or second is None:
            return None
        else:
            return round(first / second, 2)

    def update_company(self, query1, query2):
        chosen_company = self.select_company(query1)
        if chosen_company:
            new_financial = dict()
            self.new_financial(new_financial)
            for key, value in new_financial.items():
                query2.filter(Financial.ticker == chosen_company[0]).update({key: value})
            session.commit()
            print('Company updated successfully!')

    def delete_company(self, query1, query2):
        chosen_company = self.select_company(query1)
        if chosen_company:
            query2.filter(Financial.ticker == chosen_company[0]).delete()
            query1.filter(Companies.ticker == chosen_company[0]).delete()
            session.commit()
            print('Company deleted successfully!')

    @staticmethod
    def list_all_companies(query):
        print('COMPANY LIST')
        for row in query.order_by(Companies.ticker):
            print(row.ticker, row.name, row.sector)

    @staticmethod
    def top_nd():
        print(f'TICKER ND/EBITDA')
        counter, tops = 0, []
        filtering = Financial.net_debt is not None and Financial.ebitda is not None
        ordering = Financial.net_debt / Financial.ebitda
        for row in query_f.filter(filtering).order_by(desc(ordering)):
            if counter < 10:
                tops.append([row.ticker, round(row.net_debt / row.ebitda, 2)])
                counter += 1
        tops.sort(key=lambda x: (x[1], x[0]), reverse=True)
        for i in tops:
            print(i[0], i[1])

    @staticmethod
    def top_roe():
        print(f'TICKER ROE')
        counter, tops = 0, []
        filtering = Financial.net_profit is not None and Financial.equity is not None
        ordering = Financial.net_profit / Financial.equity
        for row in query_f.filter(filtering).order_by(desc(ordering)):
            if counter < 10:
                tops.append([row.ticker, round(row.net_profit / row.equity, 2)])
                counter += 1
        tops.sort(key=lambda x: (x[1], x[0]), reverse=True)
        for i in tops:
            print(i[0], i[1])

    @staticmethod
    def top_roa():
        print(f'TICKER ROA')
        counter, tops = 0, []
        filtering = Financial.net_profit is not None and Financial.assets is not None
        ordering = Financial.net_profit / Financial.assets
        for row in query_f.filter(filtering).order_by(desc(ordering)):
            if counter < 10:
                tops.append([row.ticker, round(row.net_profit / row.assets, 2)])
                counter += 1
        tops.sort(key=lambda x: (x[1], x[0]), reverse=True)
        for i in tops:
            print(i[0], i[1])


if __name__ == '__main__':
    CalculatorInvestor().main_menu()
