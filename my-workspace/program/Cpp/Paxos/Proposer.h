//Proposer.h
#include<time.h>
struct PROPOSAL
{
    unsigned int serialNum;
    int value;
};

class Proposer{
    public:
        int m_proposerCount;
        int m_acceptorCount;
        PROPOSAL m_value;
        bool m_proposeFinished;
        bool m_isAgree;
        int m_maxAcceptedSerialNum;
        int m_okCount;
        int m_refuseCount;
        int m_start;


        Proposer();
        Proposer(short proposerCount, short acceptorCount);
        void SetPlayerCount(short proposerCount, short acceptorCount);
        void StartPropose(PROPOSAL &value);
        PROPOSAL& GetProposal();
        bool Proposed(bool ok, PROPOSAL &lastAcceptValue);
        bool StartAccept();
        bool Accepted(bool ok);
        bool IsAgree();

};